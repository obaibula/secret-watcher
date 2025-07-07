package watcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

type logger interface {
	Info(msg string, args ...any)
	Debug(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type Provider struct {
	mu               *sync.RWMutex
	logger           logger
	client           kubernetes.Interface
	namespace        string
	dataBySecretName dataMapBySecretNameMap
}

func New(logger logger, client kubernetes.Interface, namespace string) *Provider {
	p := &Provider{
		mu:               &sync.RWMutex{},
		logger:           logger,
		client:           client,
		namespace:        namespace,
		dataBySecretName: make(dataMapBySecretNameMap),
	}
	return p
}

// Get returns value from secret by key, and comma-ok bool
func (p *Provider) Get(secretName, key string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.dataBySecretName[secretName] == nil {
		return "", false
	}
	val, ok := p.dataBySecretName[secretName][key]
	return string(val), ok
}

func (p *Provider) SpawnWatcherFor(ctx context.Context, secretName string) {
	rateLimiter := newRateLimiter(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				p.logger.Info("Stopped watch", slog.String("secret", secretName), slog.String("err", ctx.Err().Error()))
				return
			case <-rateLimiter:
				p.logger.Info("Starting watch", slog.String("secret", secretName))
				err := p.watch(ctx, secretName)
				if err != nil && ctx.Err() == nil {
					p.logger.Error("failed to watch, trying to restart", slog.String("secret", secretName), slog.String("err", err.Error()))
				}
			}
		}
	}()
}

// watch waits for events from k8s, on start of the watch it received watch.Added event even if secret already exists.
func (p *Provider) watch(ctx context.Context, secretName string) error {
	w, err := p.client.CoreV1().Secrets(p.namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", secretName),
	})
	if err != nil {
		return fmt.Errorf("starting watch for: %q secret, err: %w", secretName, err)
	}

	for event := range w.ResultChan() {

		switch event.Type {
		case watch.Added, watch.Modified:
			p.mu.Lock()

			p.logger.Info("received event for secret, updating the data", slog.String("event", string(event.Type)), slog.String("secret", secretName))
			secret, ok := event.Object.(*corev1.Secret)
			if !ok {
				w.Stop()
				return errors.New("event object is not secret")
			}
			p.dataBySecretName[secretName] = maps.Clone(secret.Data)

			p.mu.Unlock()
		case watch.Deleted:
			p.mu.Lock()

			p.logger.Info("received event for secret, cleaning the data", slog.String("event", string(watch.Deleted)), slog.String("secret", secretName))
			p.dataBySecretName[secretName] = nil

			p.mu.Unlock()
		case watch.Error:
			// if the ctx is cancelled, channel notifies with watch.Error immidiately
			status, _ := event.Object.(*metav1.Status)
			w.Stop()
			return fmt.Errorf("Received error event on watch. Api status: %q, code: %d, reason: %q", status.Status, status.Code, status.Reason)
		}
	}
	return nil
}
