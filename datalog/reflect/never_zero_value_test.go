package reflect_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

type personWithBio struct {
	ID    datalog.Identity `datalog:"-,id"`
	Bio string           `datalog:"bio"`
	Name  string           `datalog:"name"`
}

type entityWithTags struct {
	ID   datalog.Identity `datalog:"-,id"`
	Tags []string         `datalog:"tags"`
}

func neverZeroBioSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":person-with-bio/bio").Type(schema.TypeString).NeverZeroValue().Add().
		Attribute(":person-with-bio/name").Type(schema.TypeString).Add().
		Build()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSchemaFromStructDoesNotInferNeverZeroValue(t *testing.T) {
	s, err := dlreflect.SchemaFromStruct(personWithBio{})
	if err != nil {
		t.Fatal(err)
	}
	def := s.GetAttribute(datalog.NewKeyword(":person-with-bio/bio"))
	if def == nil {
		t.Fatal("expected :person-with-bio/bio")
	}
	if def.NeverZeroValue {
		t.Fatal("SchemaFromStruct must not infer NeverZeroValue")
	}
}

func TestSaveStructOmitsZeroFieldAndLeavesPriorValue(t *testing.T) {
	sch := neverZeroBioSchema(t)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Store:          mustOpenStore(t, mode.backend),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ent := personWithBio{Name: "Alice", Bio: "likes gardening"}
			tx := db.NewTransaction()
			id, err := tx.SaveStruct(&ent)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			ent.ID = id
			ent.Bio = ""
			tx = db.NewTransaction()
			if _, err := tx.SaveStruct(&ent); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			var loaded personWithBio
			if err := db.PullInto(id, &loaded); err != nil {
				t.Fatal(err)
			}
			if loaded.Bio != "likes gardening" {
				t.Fatalf("prior bio must remain, got %q", loaded.Bio)
			}
		})
	}
}

func TestPullIntoAbsenceIsZeroValue(t *testing.T) {
	sch := neverZeroBioSchema(t)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Store:          mustOpenStore(t, mode.backend),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ent := personWithBio{Name: "Alice"}
			tx := db.NewTransaction()
			id, err := tx.SaveStruct(&ent)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			var loaded personWithBio
			if err := db.PullInto(id, &loaded); err != nil {
				t.Fatal(err)
			}
			if loaded.Bio != "" {
				t.Fatalf("absent bio must pull as zero, got %q", loaded.Bio)
			}
			if loaded.Name != "Alice" {
				t.Fatalf("name: got %q", loaded.Name)
			}
		})
	}
}

func TestWriteAutoOmitsZeroField(t *testing.T) {
	sch := neverZeroBioSchema(t)
	bio := datalog.NewKeyword(":person-with-bio/bio")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Store:          mustOpenStore(t, mode.backend),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ent := personWithBio{Name: "Alice"}
			writer, err := dlreflect.NewStructWriter(&ent, sch)
			if err != nil {
				t.Fatal(err)
			}
			tx := db.NewTransaction()
			id, err := writer.WriteAuto(tx, &ent)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			got, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`, id, bio))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 0 {
				t.Fatalf("zero bio must write no datom, got %v", got)
			}

			var loaded personWithBio
			if err := db.PullInto(id, &loaded); err != nil {
				t.Fatal(err)
			}
			if loaded.Name != "Alice" {
				t.Fatalf("name: got %q", loaded.Name)
			}
		})
	}
}

func TestSaveStructRejectsZeroElementInNonEmptySlice(t *testing.T) {
	sch, err := schema.NewBuilder().
		Attribute(":entity-with-tags/tags").Type(schema.TypeString).Many().NeverZeroValue().Add().
		Build()
	if err != nil {
		t.Fatal(err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Store:          mustOpenStore(t, mode.backend),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			tx := db.NewTransaction()
			_, err = tx.SaveStruct(&entityWithTags{Tags: []string{"ok", ""}})
			if err == nil {
				t.Fatal("expected error for zero element in non-empty slice")
			}
		})
	}
}
