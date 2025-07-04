package secretwatcher

import (
	"context"
	"fmt"
	"time"

	"github.com/obaibula/secret-watcher/internal/watcher"
	"k8s.io/client-go/kubernetes"
)

type SecretWatcher struct {
	*watcher.Provider
}

func (w *SecretWatcher) SpawnWatchFor(secretName string) {
	fmt.Println("Watching", secretName)
	go w.WatchForSecret(context.TODO(), secretName)
	//TODO: hm...should it be in sync? or don't bother at all as we watch secret anyway? Just do eventually in tests.
	time.Sleep(time.Second)
}

func New(client kubernetes.Interface, namespace string) *SecretWatcher {
	return &SecretWatcher{Provider: watcher.New(client, namespace)}
}
