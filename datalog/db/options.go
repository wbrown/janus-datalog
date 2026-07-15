package db

import (
	"fmt"
	"os"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

type config struct {
	schema               schema.SchemaProvider
	replicaID            uint64
	annotationHandler    annotations.Handler
	disableCache         bool
	plannerOptions       *planner.PlannerOptions
	compressionThreshold int
	store                storage.Store
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

// WithPlannerOptions overrides the default planner options.
func WithPlannerOptions(opts planner.PlannerOptions) Option {
	return func(c *config) { c.plannerOptions = &opts }
}

// WithCompressionThreshold sets the compression threshold in bytes.
// Values at or above this size are transparently compressed.
// Default (when unset / 0) is 512. Use -1 to disable compression.
func WithCompressionThreshold(bytes int) Option {
	return func(c *config) { c.compressionThreshold = bytes }
}

// WithStore injects the ordered storage backend used by the database.
func WithStore(store storage.Store) Option {
	return func(c *config) { c.store = store }
}

// WithoutCompression disables transparent value compression.
func WithoutCompression() Option {
	return func(c *config) { c.compressionThreshold = -1 }
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
