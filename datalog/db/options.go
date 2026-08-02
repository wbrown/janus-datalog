package db

import (
	"fmt"
	"os"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

type config struct {
	schema               schema.SchemaProvider
	replicaID            uint64
	annotationHandler    annotations.Handler
	disableCache         bool
	plannerOptions       *planner.PlannerOptions
	compressionThreshold int
	backendName          string
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
//
// This configures the encoder Open builds for the backend it opens, so it is an
// error alongside a store passed to Open, which arrives with an encoder already.
func WithCompressionThreshold(bytes int) Option {
	return func(c *config) { c.compressionThreshold = bytes }
}

// WithBackend selects the storage backend by name — storage.AvailableBackends
// lists what a given build has, since Badger does not compile under js/wasm.
// Unset means storage.DefaultBackend for the build: Badger natively, the memory
// store under wasm.
//
// A persistent backend requires a path; an in-process one rejects it, so
// db.Open("", db.WithBackend("memory-trees")) is the in-process form.
//
// With a dump source it names where the dump lands: an in-process backend takes
// it directly, and a persistent one gets a temporary directory the database
// removes when it closes.
func WithBackend(name string) Option {
	return func(c *config) { c.backendName = name }
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
