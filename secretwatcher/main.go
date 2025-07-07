package secretwatcher

import (
	"log/slog"
	"os"

	"github.com/obaibula/secret-watcher/internal/watcher"
	"k8s.io/client-go/kubernetes"
)

type SecretWatcher struct {
	*watcher.Provider
}

func New(client kubernetes.Interface, namespace string) *SecretWatcher {
	defaultLogger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	return NewWithLogger(defaultLogger, client, namespace)
}

type Logger interface {
	Info(msg string, args ...any)
	Debug(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

func NewWithLogger(logger Logger, client kubernetes.Interface, namespace string) *SecretWatcher {
	return &SecretWatcher{Provider: watcher.New(logger, client, namespace)}
}
