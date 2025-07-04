package watcher

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type (
	dataMap                = map[string][]byte
	dataMapBySecretNameMap = map[string]dataMap
)

type Provider struct {
	mu               *sync.RWMutex
	client           kubernetes.Interface
	namespace        string
	dataBySecretName dataMapBySecretNameMap
}

func New(client kubernetes.Interface, namespace string) *Provider {
	p := &Provider{
		mu:               &sync.RWMutex{},
		client:           client,
		namespace:        namespace,
		dataBySecretName: make(dataMapBySecretNameMap),
	}
	fmt.Println("watch has started")
	return p
}

// Get returns value from secret by key, if no key present it return to "", false
func (p *Provider) Get(secretName, key string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.dataBySecretName[secretName] == nil {
		return "", false
	}
	val, ok := p.dataBySecretName[secretName][key]
	return string(val), ok
}

// watcher should trigger updates of the structure on different events
// calling provided update method of an interface
func (p *Provider) WatchForSecret(ctx context.Context, secretName string) error {
	w, err := p.client.CoreV1().Secrets(p.namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", secretName),
	})
	if err != nil {
		return fmt.Errorf("starting watch for: %q secret, err: %w", secretName, err)
	}

	for event := range w.ResultChan() {
		secret, ok := event.Object.(*corev1.Secret)
		if !ok {
			return errors.New("invalid obj")
		}

		p.mu.Lock()

		switch event.Type {
		case watch.Added, watch.Modified:
			p.dataBySecretName[secretName] = maps.Clone(secret.Data)
		case watch.Deleted:
			p.dataBySecretName[secretName] = make(dataMap)
		case watch.Error:
			w.Stop()
		}

		p.mu.Unlock()
	}
	return nil
}
