package watcher

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Provider struct {
	client     kubernetes.Interface
	namespace  string
	secretName string
}

func New(client kubernetes.Interface, namespace, secretName string) *Provider {
	p := &Provider{
		client:     client,
		namespace:  namespace,
		secretName: secretName,
	}
	go p.watchForSecret(context.TODO())
	return p
}

func (p *Provider) watchForSecret(ctx context.Context) error {
	secret, err := p.client.CoreV1().Secrets(p.namespace).Get(ctx, p.secretName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get secret: %q, in namespace: %q, err: %w", p.secretName, p.namespace, err)
	}

	for key, value := range secret.Data {
		fmt.Printf("Key: %s, Value: %s", key, value)
	}
	return nil
}
