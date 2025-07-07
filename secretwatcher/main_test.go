package secretwatcher_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/obaibula/secret-watcher/secretwatcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
)

const namespace = "test-ns"

var corev1CallsCount atomic.Int32

type countingClient struct {
	kubernetes.Interface
}

func (c countingClient) CoreV1() typedcorev1.CoreV1Interface {
	corev1CallsCount.Add(1)
	return c.Interface.CoreV1()
}

func (c countingClient) ResetCounter() {
	corev1CallsCount.Store(0)
}

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

func (s *SecretWatcherSuite) TestGet(t *testing.T) {
	countingClient := countingClient{Interface: s.client}

	t.Run("No redundant calls to the cluster", func(t *testing.T) {
		const (
			secretName = "t-name"
			key        = "t-key"
			wantValue  = "t-value"
		)

		t.Cleanup(countingClient.ResetCounter)
		t.Cleanup(func() {
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName, *metav1.NewDeleteOptions(0))
		})

		secret := s.createSecret(t, secretName, map[string][]byte{key: []byte(wantValue)})
		sw := secretwatcher.New(countingClient, namespace)
		sw.SpawnWatcherFor(t.Context(), secretName)

		assert.Eventually(t, assertSecretValue(sw, secretName, key, wantValue, true), 30*time.Second, time.Second)
		assert.Equal(t, int32(1), corev1CallsCount.Load())

		const newWantValue = "t-new-value"
		secret.Data[key] = []byte(newWantValue)
		s.updateSecret(t, secret)
		assert.Eventually(t, assertSecretValue(sw, secretName, key, newWantValue, true), 30*time.Second, time.Second)
		assert.Equal(t, int32(1), corev1CallsCount.Load())

		_, _ = sw.Get(secretName, key)
		assert.Equal(t, int32(1), corev1CallsCount.Load())
	})

	t.Run("On Create", func(t *testing.T) {
		const (
			secretName = "not-created-yet"
			key        = "t-key"
			wantValue  = "t-value"
		)

		t.Cleanup(countingClient.ResetCounter)
		t.Cleanup(func() {
			s.client.CoreV1().Secrets(namespace).Delete(s.ctx, secretName, *metav1.NewDeleteOptions(0))
		})

		sw := secretwatcher.New(countingClient, namespace)
		sw.SpawnWatcherFor(t.Context(), secretName)

		assert.Never(t, assertSecretHasValue(sw, secretName, key), 10*time.Second, time.Second)
		assert.Equal(t, int32(1), corev1CallsCount.Load())

		s.createSecret(t, secretName, map[string][]byte{key: []byte(wantValue)})

		assert.Eventually(t, assertSecretValue(sw, secretName, key, wantValue, true), 30*time.Second, time.Second)
		assert.Equal(t, int32(1), corev1CallsCount.Load())
	})

	t.Run("Cleant up all unused spawns", func(t *testing.T) {
		// TODO: this should be fulfilled
		assert.Never(t, func() bool { return corev1CallsCount.Load() != 0 }, 10*time.Second, time.Second)
	})
}

func assertSecretValue(sw *secretwatcher.SecretWatcher, secretName, key, wantValue string, wantOk bool) func() bool {
	return func() bool {
		gotValue, gotOk := sw.Get(secretName, key)
		return wantValue == gotValue && wantOk == gotOk
	}
}

func assertSecretHasValue(sw *secretwatcher.SecretWatcher, secretName, key string) func() bool {
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
