package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// branchScans records the bound of every scan a branch's store opens, itself
// and through its read sessions.
type branchScans struct {
	sync.Mutex
	opened []branchScan
}

// branchScan is one scan a branch's store opened: its bound, and whether it
// read keys alone.
type branchScan struct {
	bound    ScanBound
	keysOnly bool
}

func (s *branchScans) record(bound ScanBound, keysOnly bool) {
	s.Lock()
	defer s.Unlock()
	s.opened = append(s.opened, branchScan{bound: bound, keysOnly: keysOnly})
}

// declarationReads is how many of the scans read the definitions' refs to
// their declarations: the scan the definitions query opens, once per read of
// the schema's facts.
func (s *branchScans) declarationReads() int {
	s.Lock()
	defer s.Unlock()
	reads := 0
	for _, scan := range s.opened {
		if len(scan.bound.Prefix) > 0 && scan.bound.Prefix[0] == datalog.Value(attributeDeclarationAttr) {
			reads++
		}
	}
	return reads
}

// inferences is how many of the scans read the whole of ATEV by its keys: the
// pass inferSchemaFromStore makes, once per inference.
func (s *branchScans) inferences() int {
	s.Lock()
	defer s.Unlock()
	passes := 0
	for _, scan := range s.opened {
		if scan.keysOnly && scan.bound.Index == ATEV && len(scan.bound.Prefix) == 0 {
			passes++
		}
	}
	return passes
}

// forkScanStore is a store whose forks record their scans: it reads as the
// store it wraps, and the store it hands a branch records into scans.
type forkScanStore struct {
	Store
	scans *branchScans
}

func (s forkScanStore) Fork(ceiling datalog.ElementID) (Store, error) {
	source, ok := s.Store.(forker)
	if !ok {
		return nil, fmt.Errorf("a %T store cannot fork a branch", s.Store)
	}
	forked, err := source.Fork(ceiling)
	if err != nil {
		return nil, err
	}
	return recordedBranchStore{Store: forked, scans: s.scans}, nil
}

// recordedBranchStore is a branch's store recording the bound of every scan
// it and its read sessions open.
type recordedBranchStore struct {
	Store
	scans *branchScans
}

func (s recordedBranchStore) Scan(bound ScanBound) (Iterator, error) {
	s.scans.record(bound, false)
	return s.Store.Scan(bound)
}

func (s recordedBranchStore) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	s.scans.record(bound, true)
	return s.Store.ScanKeysOnly(bound)
}

func (s recordedBranchStore) NewReadSession() (ReadSession, error) {
	session, err := s.Store.NewReadSession()
	if err != nil {
		return nil, err
	}
	return recordedBranchSession{ReadSession: session, scans: s.scans}, nil
}

type recordedBranchSession struct {
	ReadSession
	scans *branchScans
}

func (s recordedBranchSession) Scan(bound ScanBound) (Iterator, error) {
	s.scans.record(bound, false)
	return s.ReadSession.Scan(bound)
}

func (s recordedBranchSession) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	s.scans.record(bound, true)
	return s.ReadSession.ScanKeysOnly(bound)
}

// A fork composes its branch's schema once. The branch's store is its
// parent's as of the snapshot, so where the parent's given schema and its
// declared definitions are already stated there, the fork reads the schema's
// facts once and infers from the store's ops once, and the branch runs with
// what its store states: the given schema's definitions, and the definition
// the ops infer for an attribute written before the snapshot and never
// declared, while an attribute first written after the snapshot has none.
// Where the parent declared an attribute after the snapshot, the fork asserts
// that declaration into the branch and reads the facts once more, after the
// write, and the branch runs with the declaration.
func TestAForkComposesItsBranchsSchemaOnce(t *testing.T) {
	given, err := schema.NewBuilder().
		Attribute(":fork/name").Type(schema.TypeString).Add().
		Attribute(":fork/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	widened, err := schema.NewBuilder().
		Attribute(":fork/name").Type(schema.TypeString).Add().
		Attribute(":fork/tags").Type(schema.TypeString).Many().Add().
		Attribute(":fork/late").Type(schema.TypeLong).Many().Add().
		Build()
	require.NoError(t, err)
	name, tags := datalog.NewKeyword(":fork/name"), datalog.NewKeyword(":fork/tags")
	loose, after, late := datalog.NewKeyword(":fork/loose"), datalog.NewKeyword(":fork/after"), datalog.NewKeyword(":fork/late")
	entity := datalog.NewIdentity("fork:e")
	write := func(t *testing.T, d *Database, attribute datalog.Keyword, value interface{}) {
		t.Helper()
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(entity, attribute, value))
		_, err := tx.Commit()
		require.NoError(t, err)
	}
	cases := []struct {
		name string
		// declares is what the parent does between the snapshot and the fork.
		declares func(t *testing.T, parent *Database)
		// reads is how many times the fork reads the schema's facts.
		reads int
		// lateCardinality is the cardinality the branch runs :fork/late under,
		// nil where it has no definition of it.
		lateCardinality datalog.Keyword
	}{
		{"the branch's store states every definition", func(t *testing.T, parent *Database) {}, 1, nil},
		{"the parent declared an attribute after the snapshot", func(t *testing.T, parent *Database) {
			require.NoError(t, parent.SetSchema(widened))
		}, 2, schema.CardinalityMany},
	}
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				scans := &branchScans{}
				base, err := mode.backend.Open(t.TempDir(), newBinaryKeyEncoder(0))
				require.NoError(t, err)
				plannerOptions := mode.plannerOptions()
				parent, err := NewDatabaseWithOptions(DatabaseOptions{
					Store:             forkScanStore{Store: base, scans: scans},
					Schema:            given,
					PlannerOptions:    &plannerOptions,
					AnnotationHandler: recordingReach(t, nil),
				})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, parent.Close()) })
				write(t, parent, name, "Alice")
				write(t, parent, tags, "a")
				write(t, parent, loose, "undeclared")
				_, err = parent.Snapshot("s")
				require.NoError(t, err)
				write(t, parent, after, "undeclared, and after the snapshot")
				c.declares(t, parent)

				branch := branchOf(t, parent, "s")
				assert.Equal(t, c.reads, scans.declarationReads(), "the fork's reads of the schema's facts")
				assert.Equal(t, 1, scans.inferences(), "the fork's passes over the store's ops")

				running := branch.Schema()
				require.Same(t, schema.CardinalityOne, running.GetAttribute(name).Cardinality, "a definition of the given schema")
				require.Same(t, schema.CardinalityMany, running.GetAttribute(tags).Cardinality, "a definition of the given schema")
				inferred := running.GetAttribute(loose)
				require.NotNil(t, inferred, "an attribute written before the snapshot and never declared is inferred from the branch's ops")
				require.Same(t, schema.CardinalityOne, inferred.Cardinality)
				require.Nil(t, running.GetAttribute(after), "an attribute first written after the snapshot has no datom in the branch")
				if c.lateCardinality == nil {
					require.Nil(t, running.GetAttribute(late), "an attribute the parent never declared")
				} else {
					declared := running.GetAttribute(late)
					require.NotNil(t, declared, "a declaration the parent made after the snapshot reaches the branch")
					require.Same(t, c.lateCardinality, declared.Cardinality)
				}
			})
		}
	}
}
