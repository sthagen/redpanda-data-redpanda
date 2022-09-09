package v1alpha1

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SecretKeyRef contains enough information to inspect or modify the referred Secret data
// REF https://pkg.go.dev/k8s.io/api/core/v1#ObjectReference
type SecretKeyRef struct {
	// Name of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string `json:"name"`

	// Namespace of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
	Namespace string `json:"namespace"`

	// +optional
	// Key in Secret data to get value from
	Key string `json:"key,omitempty"`
}

// GetSecret fetches the referenced Secret
func (s *SecretKeyRef) GetSecret(ctx context.Context, cl client.Client) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	if err := cl.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Name}, secret); err != nil {
		return nil, fmt.Errorf("getting Secret %s/%s: %w", s.Namespace, s.Name, err)
	}
	return secret, nil
}

// GetValue extracts the value from the specified key or default
func (s *SecretKeyRef) GetValue(secret *corev1.Secret, defaultKey string) ([]byte, error) {
	key := s.Key
	if key == "" {
		key = defaultKey
	}

	value, ok := secret.Data[key]
	if !ok {
		return nil, fmt.Errorf("getting value from Secret %s/%s: key %s not found", s.Namespace, s.Name, key) // nolint:goerr113 // no need to declare new error type
	}
	return value, nil
}

// NamespaceNameRef contains namespace and name to inspect or modify the referred object
// REF https://pkg.go.dev/k8s.io/api/core/v1#ObjectReference
type NamespaceNameRef struct {
	// Name of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string `json:"name"`

	// Namespace of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
	Namespace string `json:"namespace"`
}
