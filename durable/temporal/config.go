package temporal

import (
	"crypto/tls"
	"fmt"
	"os"

	"go.temporal.io/sdk/client"
)

// Config configures the Temporal client. It is environment-driven so the same
// binary targets a local dev server or Temporal Cloud without code changes.
type Config struct {
	// Address is the Temporal frontend host:port. Default: 127.0.0.1:7233.
	Address string
	// Namespace is the Temporal namespace. Default: "default".
	Namespace string
	// TaskQueue is the queue workers poll and workflows are scheduled on.
	TaskQueue string
	// APIKey enables Temporal Cloud API-key auth (TLS). Mutually exclusive
	// with TLS cert/key.
	APIKey string
	// TLSCertPath / TLSKeyPath enable mTLS (Temporal Cloud or self-hosted).
	TLSCertPath string
	TLSKeyPath  string
}

// ConfigFromEnv builds a Config from the standard environment variables:
//
//	TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_TASK_QUEUE,
//	TEMPORAL_API_KEY, TEMPORAL_TLS_CERT, TEMPORAL_TLS_KEY
//
// Unset fields fall back to dev-server defaults (localhost:7233, namespace
// "default", task queue "tacklr").
func ConfigFromEnv() Config {
	return Config{
		Address:     os.Getenv("TEMPORAL_ADDRESS"),
		Namespace:   os.Getenv("TEMPORAL_NAMESPACE"),
		TaskQueue:   os.Getenv("TEMPORAL_TASK_QUEUE"),
		APIKey:      os.Getenv("TEMPORAL_API_KEY"),
		TLSCertPath: os.Getenv("TEMPORAL_TLS_CERT"),
		TLSKeyPath:  os.Getenv("TEMPORAL_TLS_KEY"),
	}
}

const (
	defaultAddress   = "127.0.0.1:7233"
	defaultNamespace = "default"
	defaultTaskQueue = "tacklr"
)

// ClientOptions converts the Config to Temporal client options.
func (c Config) ClientOptions() (client.Options, error) {
	opts := client.Options{
		HostPort:  firstNonEmpty(c.Address, defaultAddress),
		Namespace: firstNonEmpty(c.Namespace, defaultNamespace),
	}
	if c.APIKey != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(c.APIKey)
		// API-key auth requires TLS.
		opts.ConnectionOptions.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		return opts, nil
	}
	if c.TLSCertPath != "" || c.TLSKeyPath != "" {
		if c.TLSCertPath == "" || c.TLSKeyPath == "" {
			return client.Options{}, fmt.Errorf("temporal: both TLS cert and key are required for mTLS")
		}
		cert, err := tls.LoadX509KeyPair(c.TLSCertPath, c.TLSKeyPath)
		if err != nil {
			return client.Options{}, fmt.Errorf("temporal: load TLS key pair: %w", err)
		}
		opts.ConnectionOptions.TLS = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		}
	}
	return opts, nil
}

// TaskQueueName returns the configured task queue, or the default.
func (c Config) TaskQueueName() string {
	return firstNonEmpty(c.TaskQueue, defaultTaskQueue)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
