package session

import "context"

// addToSessionOptionsKey is the context key used to store AddToSessionOptions.
type addToSessionOptionsKey struct{}

// AddToSessionOptions controls how SessionManager.AddToSession behaves.
type AddToSessionOptions struct {
	// SkipStore prevents the session from persisting the packet. The packet will
	// not be retried after a reconnect and any in-flight state is discarded if
	// the connection drops before the transaction completes.
	SkipStore bool
}

// WithAddToSessionOptions returns a context that carries the supplied
// AddToSessionOptions.
func WithAddToSessionOptions(ctx context.Context, opts AddToSessionOptions) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, addToSessionOptionsKey{}, opts)
}

// addToSessionOptionsFromContext extracts AddToSessionOptions from the context.
// The zero value is returned if no options are present.
func addToSessionOptionsFromContext(ctx context.Context) AddToSessionOptions {
	if ctx == nil {
		return AddToSessionOptions{}
	}
	if v, ok := ctx.Value(addToSessionOptionsKey{}).(AddToSessionOptions); ok {
		return v
	}
	return AddToSessionOptions{}
}

// AddToSessionOptionsFromContext exposes the helper for implementation specific
// packages that need to inspect AddToSessionOptions values stored on the
// context. External callers should prefer WithAddToSessionOptions.
func AddToSessionOptionsFromContext(ctx context.Context) AddToSessionOptions {
	return addToSessionOptionsFromContext(ctx)
}
