package watcher_test

import (
	"context"
	"log/slog"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/obaibula/secret-watcher/watcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"

	"slices"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
)

const namespace = "test-ns"

type SecretWatcherSuite struct {
	client kubernetes.Interface
	ctx    context.Context
}

func (s *SecretWatcherSuite) Setup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)

	k3sContainer, err := k3s.Run(ctx, "rancher/k3s:v1.27.1-k3s1")
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, k3sContainer)

	kubeConfigYaml, err := k3sContainer.GetKubeConfig(ctx)
	require.NoError(t, err)

	restcfg, err := clientcmd.RESTConfigFromKubeConfig(kubeConfigYaml)
	require.NoError(t, err)

	client, err := kubernetes.NewForConfig(restcfg)
	require.NoError(t, err)

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}
	_, err = client.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	require.NoError(t, err)

	s.client = client
	s.ctx = ctx
}

func TestSecretWatcherSuite(t *testing.T) {
	s := &SecretWatcherSuite{}
	s.Setup(t)

	t.Run("Get", s.TestGet)
}

type countingClient struct {
	kubernetes.Interface
	corev1CallsCount atomic.Int32
}

func (c *countingClient) CoreV1() typedcorev1.CoreV1Interface {
	c.corev1CallsCount.Add(1)
	return c.Interface.CoreV1()
}

func (c *countingClient) getCoreV1Count() int {
	return int(c.corev1CallsCount.Load())
}

type loggerSpy struct {
	*slog.Logger
	mu      *sync.RWMutex
	records []string
}

func newLoggerSpy(logger *slog.Logger) *loggerSpy {
	return &loggerSpy{
		Logger: logger,
		mu:     &sync.RWMutex{},
	}
}

func (log *loggerSpy) Info(msg string, args ...any) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.Logger.Info(msg, args...)

	// only collecting string args, to check on text presence
	var strArgs string
	for _, arg := range args {
		switch a := arg.(type) {
		case string:
			strArgs += a
		case slog.Attr:
			strArgs += a.Value.String()
		}
	}

	logLine := msg + strArgs
	log.records = append(log.records, logLine)
}

func (log *loggerSpy) assertInfoContains(rx *regexp.Regexp) func() bool {
	return func() bool {
		log.mu.RLock()
		defer log.mu.RUnlock()
		return slices.ContainsFunc(log.records, rx.MatchString)
	}
}

func (log *loggerSpy) getInfoLogs() []string {
	log.mu.RLock()
	defer log.mu.RUnlock()
	return slices.Clone(log.records)
}

func (s *SecretWatcherSuite) TestGet(t *testing.T) {
	const (
		// so long waitFor is used to prevent flaky tests on very slow and busy ci,
		// on any successful tick assert.Eventually unblocks, so if ci is fast -> test runs fast
		eventuallyWaitFor = 2 * time.Minute
		eventuallyTick    = time.Second
		neverWaitFor      = 10 * time.Second
		neverTick         = time.Second
	)

	t.Run("On Create", func(t *testing.T) {
		const (
			secretName = "not-created-yet"
			key        = "t-key"
			wantValue  = "t-value"
		)

		t.Cleanup(func() {
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName, *metav1.NewDeleteOptions(0))
		})

		countingClient := &countingClient{Interface: s.client}
		sw := watcher.New(countingClient, namespace)
		sw.SpawnWatcherFor(t.Context(), secretName)

		assert.Never(t, assertSecretHasValue(sw, secretName, key), neverWaitFor, neverTick)
		assert.Equal(t, 1, countingClient.getCoreV1Count())

		s.createSecret(t, secretName, map[string][]byte{key: []byte(wantValue)})

		assert.Eventually(t, assertSecretValue(sw, secretName, key, wantValue, true), eventuallyWaitFor, eventuallyTick)
		assert.Equal(t, 1, countingClient.getCoreV1Count())
	})

	t.Run("On Update", func(t *testing.T) {
		const (
			secretName = "s-n"
			key1       = "s-k"
			wantValue1 = "s-v"
			key2       = "s-k-2"
			wantValue2 = "s-v-2"
			key3       = "s-k-3"
			wantValue3 = "s-v-3"
		)

		t.Cleanup(func() {
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName, *metav1.NewDeleteOptions(0))
		})

		secret := s.createSecret(t, secretName, map[string][]byte{key1: []byte(wantValue1)})
		countingClient := &countingClient{Interface: s.client}
		sw := watcher.New(countingClient, namespace)
		sw.SpawnWatcherFor(t.Context(), secretName)

		assert.Eventually(t, assertSecretValue(sw, secretName, key1, wantValue1, true), eventuallyWaitFor, eventuallyTick)
		assert.Equal(t, 1, countingClient.getCoreV1Count())

		const newWantValue = "t-new-value"
		secret.Data[key1] = []byte(newWantValue)
		secret = s.updateSecret(t, secret)
		assert.Eventually(t, assertSecretValue(sw, secretName, key1, newWantValue, true), eventuallyWaitFor, eventuallyTick)
		assert.Equal(t, 1, countingClient.getCoreV1Count())

		secret.Data = map[string][]byte{
			key1: []byte(wantValue1),
			key2: []byte(wantValue2),
			key3: []byte(wantValue3),
		}
		secret = s.updateSecret(t, secret)
		assert.Eventually(t, assertSecretValue(sw, secretName, key1, wantValue1, true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName, key2, wantValue2, true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName, key3, wantValue3, true), eventuallyWaitFor, eventuallyTick)
		assert.Equal(t, 1, countingClient.getCoreV1Count())

		secret.Data = map[string][]byte{
			key2: []byte(wantValue2),
		}
		secret = s.updateSecret(t, secret)
		assert.Eventually(t, assertSecretValue(sw, secretName, key1, "", false), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName, key2, wantValue2, true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName, key3, "", false), eventuallyWaitFor, eventuallyTick)
		assert.Equal(t, 1, countingClient.getCoreV1Count())

		secret.Data = map[string][]byte{
			key2: {},
			key3: []byte(""),
		}
		s.updateSecret(t, secret)
		assert.Eventually(t, assertSecretValue(sw, secretName, key1, "", false), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName, key2, "", true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName, key3, "", true), eventuallyWaitFor, eventuallyTick)
		assert.Equal(t, 1, countingClient.getCoreV1Count())
	})

	t.Run("On Delete", func(t *testing.T) {
		const (
			secretName = "some-disctint-secret-name"
			key        = "d-k"
			wantValue  = "d-v"
		)

		s.createSecret(t, secretName, map[string][]byte{key: []byte(wantValue)})
		countingClient := &countingClient{Interface: s.client}
		sw := watcher.New(countingClient, namespace)
		sw.SpawnWatcherFor(t.Context(), secretName)

		assert.Eventually(t, assertSecretValue(sw, secretName, key, wantValue, true), eventuallyWaitFor, eventuallyTick)
		assert.Equal(t, 1, countingClient.getCoreV1Count())

		s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName, *metav1.NewDeleteOptions(0))
		_, err := s.client.CoreV1().Secrets(namespace).Get(s.ctx, secretName, *&metav1.GetOptions{})
		var statusErr *k8sErrors.StatusError
		assert.ErrorAs(t, err, &statusErr)
		assert.Equal(t, statusErr.Status().Reason, metav1.StatusReasonNotFound)
		assert.Contains(t, statusErr.Status().Message, secretName)
		assert.Equal(t, 1, countingClient.getCoreV1Count())
	})

	t.Run("No redundant calls to the cluster", func(t *testing.T) {
		const (
			secretName = "t-name"
			key        = "t-key"
			wantValue  = "t-value"
		)

		t.Cleanup(func() {
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName, *metav1.NewDeleteOptions(0))
		})

		secret := s.createSecret(t, secretName, map[string][]byte{key: []byte(wantValue)})
		countingClient := &countingClient{Interface: s.client}
		sw := watcher.New(countingClient, namespace)
		sw.SpawnWatcherFor(t.Context(), secretName)

		assert.Eventually(t, assertSecretValue(sw, secretName, key, wantValue, true), eventuallyWaitFor, eventuallyTick)
		assert.Equal(t, 1, countingClient.getCoreV1Count())

		const newWantValue = "t-new-value"
		secret.Data[key] = []byte(newWantValue)
		s.updateSecret(t, secret)
		assert.Eventually(t, assertSecretValue(sw, secretName, key, newWantValue, true), eventuallyWaitFor, eventuallyTick)
		assert.Equal(t, 1, countingClient.getCoreV1Count())

		_, _ = sw.Get(secretName, key)
		assert.Equal(t, 1, countingClient.getCoreV1Count())
	})

	t.Run("Comprehensive: multiple secrets operations", func(t *testing.T) {
		const (
			secretName1 = "n-1"
			secretName2 = "n-2"
			secretName3 = "n-3"
			key1        = "k"
			wantValue1  = "v"
			key2        = "k-2"
			wantValue2  = "v-2"
			key3        = "k-3"
			wantValue3  = "v-3"
		)

		t.Cleanup(func() {
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName1, *metav1.NewDeleteOptions(0))
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName2, *metav1.NewDeleteOptions(0))
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName3, *metav1.NewDeleteOptions(0))
		})

		secret1 := s.createSecret(t, secretName1, map[string][]byte{key1: []byte(wantValue1)})
		countingClient := &countingClient{Interface: s.client}
		sw := watcher.New(countingClient, namespace)
		sw.SpawnWatcherFor(t.Context(), secretName1)

		sw.SpawnWatcherFor(t.Context(), secretName2)
		secret2 := s.createSecret(t, secretName2, map[string][]byte{key2: []byte(wantValue2)})

		// for the third secret create a ctx with cancel func, do cancel it prematuraly
		secret3 := s.createSecret(t, secretName3, map[string][]byte{key3: []byte(wantValue3)})
		secret3Ctx, cancelSecret3Ctx := context.WithCancel(t.Context())
		t.Cleanup(cancelSecret3Ctx)
		sw.SpawnWatcherFor(secret3Ctx, secretName3)

		assert.Eventually(t, assertSecretValue(sw, secretName1, key1, wantValue1, true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName2, key2, wantValue2, true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName3, key3, wantValue3, true), eventuallyWaitFor, eventuallyTick)

		// Secret3Ctx is cancelled, no changes must be recorded for this secret, other secrets should work as expected
		cancelSecret3Ctx()
		secret3.Data[key2] = []byte(wantValue2)
		secret3 = s.updateSecret(t, secret3)
		assert.Never(t, assertSecretHasValue(sw, secretName3, key2), neverWaitFor, neverTick)

		secret1.Data = map[string][]byte{
			key1: []byte(wantValue1),
			key2: []byte(wantValue2),
			key3: []byte(wantValue3),
		}
		secret1 = s.updateSecret(t, secret1)
		assert.Eventually(t, assertSecretValue(sw, secretName1, key1, wantValue1, true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName1, key2, wantValue2, true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName1, key3, wantValue3, true), eventuallyWaitFor, eventuallyTick)

		secret2.Data = map[string][]byte{
			key1: []byte(wantValue1),
			key2: []byte(wantValue2),
		}
		secret2 = s.updateSecret(t, secret2)
		assert.Eventually(t, assertSecretValue(sw, secretName2, key1, wantValue1, true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName2, key2, wantValue2, true), eventuallyWaitFor, eventuallyTick)
		assert.Equal(t, 3, countingClient.getCoreV1Count())
	})

	t.Run("Context cancellation kills all spawns", func(t *testing.T) {
		const (
			secretName1 = "n-1"
			secretName2 = "n-2"
			secretName3 = "n-3"
			key1        = "k"
			wantValue1  = "v"
			key2        = "k-2"
			wantValue2  = "v-2"
			key3        = "k-3"
			wantValue3  = "v-3"
		)

		t.Cleanup(func() {
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName1, *metav1.NewDeleteOptions(0))
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName2, *metav1.NewDeleteOptions(0))
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName3, *metav1.NewDeleteOptions(0))
		})

		secret1 := s.createSecret(t, secretName1, map[string][]byte{key1: []byte(wantValue1)})
		secret2 := s.createSecret(t, secretName2, map[string][]byte{key2: []byte(wantValue2)})
		secret3 := s.createSecret(t, secretName3, map[string][]byte{key3: []byte(wantValue3)})
		logSpy := newLoggerSpy(slog.New(slog.NewTextHandler(os.Stdout, nil)))
		sw := watcher.NewWithLogger(logSpy, s.client, namespace)

		ctx, cancelCtx := context.WithCancel(t.Context())
		t.Cleanup(cancelCtx)

		sw.SpawnWatcherFor(ctx, secretName1)
		sw.SpawnWatcherFor(ctx, secretName2)
		sw.SpawnWatcherFor(ctx, secretName3)
		assert.Eventually(t, assertSecretValue(sw, secretName1, key1, wantValue1, true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName2, key2, wantValue2, true), eventuallyWaitFor, eventuallyTick)
		assert.Eventually(t, assertSecretValue(sw, secretName3, key3, wantValue3, true), eventuallyWaitFor, eventuallyTick)

		cancelCtx()

		const newWantValue = "t-new-value"
		secret1.Data[key1] = []byte(newWantValue)
		secret2.Data[key2] = []byte(newWantValue)
		secret3.Data[key3] = []byte(newWantValue)
		s.updateSecret(t, secret1)
		s.updateSecret(t, secret2)
		s.updateSecret(t, secret3)
		assert.Never(t, func() bool {
			return assertSecretValue(sw, secretName1, key1, newWantValue, true)() ||
				assertSecretValue(sw, secretName2, key2, newWantValue, true)() ||
				assertSecretValue(sw, secretName3, key3, newWantValue, true)()
		}, neverWaitFor, neverTick)

		rxStrSecret1 := "Stopped watch.*" + secretName1
		rxSecret1, err := regexp.Compile(rxStrSecret1)
		require.NoError(t, err)
		assert.Eventuallyf(t, logSpy.assertInfoContains(rxSecret1), eventuallyWaitFor, eventuallyTick, "expected stopped watch for secret %q, in: %v", secretName1, logSpy.getInfoLogs())

		rxStrSecret2 := "Stopped watch.*" + secretName2
		rxSecret2, err := regexp.Compile(rxStrSecret2)
		require.NoError(t, err)
		assert.Eventuallyf(t, logSpy.assertInfoContains(rxSecret2), eventuallyWaitFor, eventuallyTick, "expected stopped watch for secret %q, in: %v", secretName2, logSpy.getInfoLogs())

		rxStrSecret3 := "Stopped watch.*" + secretName3
		rxSecret3, err := regexp.Compile(rxStrSecret3)
		require.NoError(t, err)
		assert.Eventuallyf(t, logSpy.assertInfoContains(rxSecret3), eventuallyWaitFor, eventuallyTick, "expected stopped watch for secret %q, in: %v", secretName3, logSpy.getInfoLogs())

	})
}

func assertSecretValue(sw *watcher.Watcher, secretName, key, wantValue string, wantOk bool) func() bool {
	return func() bool {
		gotValue, gotOk := sw.Get(secretName, key)
		return wantValue == gotValue && wantOk == gotOk
	}
}

func assertSecretHasValue(sw *watcher.Watcher, secretName, key string) func() bool {
	return func() bool {
		gotValue, gotOk := sw.Get(secretName, key)
		return gotValue != "" || gotOk
	}
}

func (s *SecretWatcherSuite) createSecret(t *testing.T, secretName string, data map[string][]byte) *corev1.Secret {
	t.Helper()
	t.Logf("Creating %q secret, with data %+v", secretName, data)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: secretName,
		},
		Data: data,
		Type: corev1.SecretTypeOpaque,
	}
	secret, err := s.client.CoreV1().Secrets(namespace).Create(s.ctx, secret, metav1.CreateOptions{})
	require.NoError(t, err)
	return secret
}

func (s *SecretWatcherSuite) updateSecret(t *testing.T, secret *corev1.Secret) *corev1.Secret {
	t.Helper()
	t.Logf("Updating %q secret with data %+v", secret.Name, secret.Data)
	secret, err := s.client.CoreV1().Secrets(namespace).Update(s.ctx, secret, metav1.UpdateOptions{})
	require.NoError(t, err)
	return secret
}
