package db

import (
	"fmt"
	"os"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

type config struct {
	schema            schema.SchemaProvider
	replicaID         uint64
	annotationHandler annotations.Handler
	disableCache      bool
}

// Option configures a database opened with Open.
type Option func(*config)

// WithSchema sets the schema for type validation and cardinality handling.
func WithSchema(s schema.SchemaProvider) Option {
	return func(c *config) { c.schema = s }
}

// WithReplicaID sets the CRDT replica identifier.
// Zero means auto-generate a random ID.
func WithReplicaID(id uint64) Option {
	return func(c *config) { c.replicaID = id }
}

// WithAnnotationHandler sets a handler for query tracing and observability.
func WithAnnotationHandler(h annotations.Handler) Option {
	return func(c *config) { c.annotationHandler = h }
}

// WithoutCache disables the EA cache; queries resolve directly from storage.
func WithoutCache() Option {
	return func(c *config) { c.disableCache = true }
}

// WithVerbose enables query tracing to stdout.
func WithVerbose() Option {
	return func(c *config) {
		formatter := annotations.NewOutputFormatter(os.Stdout)
		c.annotationHandler = func(event annotations.Event) {
			fmt.Fprintln(os.Stdout, formatter.Format(event))
		}
	}
}

// WithVerboseCallback enables query tracing with a custom callback.
// The callback receives pre-formatted human-readable strings.
func WithVerboseCallback(fn func(string)) Option {
	return func(c *config) {
		formatter := annotations.NewOutputFormatter(nil)
		c.annotationHandler = func(event annotations.Event) {
			fn(formatter.Format(event))
		}
	}
}
