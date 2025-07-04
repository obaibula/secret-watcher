package secretwatcher

import (
	"fmt"

	"github.com/obaibula/secret-watcher/internal/watcher"
	"k8s.io/client-go/kubernetes"
)

type SecretWatcher struct {
	*watcher.Provider
}

func (w *SecretWatcher) Watch(secretName string) {
	fmt.Println("Watching", secretName)
}

func (w *SecretWatcher) Get(secretName, key string) string {
	return "the value"
}

func New(client kubernetes.Interface, namespace, secretName string) *SecretWatcher {
	return &SecretWatcher{Provider: watcher.New(client, namespace, secretName)}
}
