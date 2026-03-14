package parser

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/ebnf/parse"
	"github.com/wbrown/janus-datalog/datalog/edn"
)

// EDN value types for transform output
type ednKeyword string
type ednSymbol string
type ednList []interface{}
type ednVector []interface{}

var testTransforms = parse.TransformMap{
	"nil_lit": func(s string) interface{} { return nil },
	"boolean": func(s string) interface{} { return s == "true" },
	"integer": func(s string) interface{} {
		v, _ := strconv.ParseInt(s, 10, 64)
		return v
	},
	"float": func(parts ...string) interface{} {
		s := strings.Join(parts, "")
		v, _ := strconv.ParseFloat(s, 64)
		return v
	},
	"string": func(s string) interface{} {
		if len(s) >= 2 && s[0] == '"' {
			s = s[1 : len(s)-1]
		}
		s = strings.ReplaceAll(s, `\"`, `"`)
		s = strings.ReplaceAll(s, `\\`, `\`)
		s = strings.ReplaceAll(s, `\n`, "\n")
		s = strings.ReplaceAll(s, `\t`, "\t")
		return s
	},
	"character": func(s string) interface{} { return s },
	"keyword":   func(s string) interface{} { return ednKeyword(s) },
	"symbol":    func(s string) interface{} { return ednSymbol(s) },
	"list":      func(items ...interface{}) interface{} { return ednList(items) },
	"vector":    func(items ...interface{}) interface{} { return ednVector(items) },
	"tagged": func(tag string, value interface{}) interface{} {
		return []interface{}{tag, value}
	},
	"tag_name": func(s string) string { return s },
}

func TestEDNAdapter_BasicTypes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  interface{}
	}{
		{"integer", "42", int64(42)},
		{"negative", "-7", int64(-7)},
		{"float", "3.14", float64(3.14)},
		{"string", `"hello"`, "hello"},
		{"keyword", ":foo", ednKeyword(":foo")},
		{"symbol", "bar", ednSymbol("bar")},
		{"true", "true", true},
		{"false", "false", false},
		{"nil", "nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := edn.Parse(tt.input)
			require.NoError(t, err)

			tree := EDNToParseTree(node, tt.input)
			result, err := parse.Transform(tree, testTransforms)
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestEDNAdapter_Collections(t *testing.T) {
	tests := []struct {
		name  string
		input string
		check func(t *testing.T, result interface{})
	}{
		{
			name:  "vector",
			input: "[1 2 3]",
			check: func(t *testing.T, result interface{}) {
				v, ok := result.(ednVector)
				require.True(t, ok, "expected ednVector, got %T", result)
				assert.Len(t, v, 3)
				assert.Equal(t, int64(1), v[0])
				assert.Equal(t, int64(2), v[1])
				assert.Equal(t, int64(3), v[2])
			},
		},
		{
			name:  "list",
			input: "(+ 1 2)",
			check: func(t *testing.T, result interface{}) {
				l, ok := result.(ednList)
				require.True(t, ok, "expected ednList, got %T", result)
				assert.Len(t, l, 3)
				assert.Equal(t, ednSymbol("+"), l[0])
			},
		},
		{
			name:  "nested",
			input: "[:find ?e :where [?e :name ?n]]",
			check: func(t *testing.T, result interface{}) {
				v, ok := result.(ednVector)
				require.True(t, ok, "expected ednVector, got %T", result)
				assert.Equal(t, ednKeyword(":find"), v[0])
				assert.Equal(t, ednSymbol("?e"), v[1])
				assert.Equal(t, ednKeyword(":where"), v[2])
				// Nested vector
				inner, ok := v[3].(ednVector)
				require.True(t, ok)
				assert.Len(t, inner, 3)
			},
		},
		{
			name:  "tagged",
			input: `#inst "2025-01-01T00:00:00Z"`,
			check: func(t *testing.T, result interface{}) {
				tagged, ok := result.([]interface{})
				require.True(t, ok, "expected tagged pair, got %T", result)
				assert.Equal(t, "inst", tagged[0])
				assert.Equal(t, "2025-01-01T00:00:00Z", tagged[1])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := edn.Parse(tt.input)
			require.NoError(t, err)

			tree := EDNToParseTree(node, tt.input)
			result, err := parse.Transform(tree, testTransforms)
			require.NoError(t, err)
			tt.check(t, result)
		})
	}
}

func TestEDNAdapter_MultipleTopLevel(t *testing.T) {
	input := "42 :foo [1 2]"
	lexer := edn.NewLexer(input)
	require.NoError(t, lexer.Lex())
	p := edn.NewParser(lexer)
	nodes, err := p.ParseAll()
	require.NoError(t, err)
	require.Len(t, nodes, 3)

	tree := EDNNodesToParseTree(nodes, input)
	require.NotNil(t, tree.Root)
	assert.Equal(t, "edn", tree.Root.Rule)
	assert.Len(t, tree.Root.Children, 3)
}

// BenchmarkEDNAdapter compares three paths:
//   - handrolled: edn.Parse only (current production)
//   - ebnf: ebnf parser from grammar (potential replacement)
//   - adapter: edn.Parse + adapt to parse.Node + transform (best of both)
func BenchmarkEDNAdapter(b *testing.B) {
	inputs := map[string]string{
		"datom":   `[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/name "Alice" 1]`,
		"simple":  `[:find ?e ?name :where [?e :person/name ?name]]`,
		"complex": `[:find ?scenario ?title ?count :where [?scenario :entity/type :entity.type/scenario] [(get-else $ ?scenario :scenario/title "") ?title] (or [(q [:find (count ?t) :in $ ?s :where [?t :task/root ?s] [?t :task/status :status/complete]] $ ?scenario) [[?count]]] [(ground 0) ?count])]`,
	}

	ednGrammar := loadEDNGrammar(b)
	ebnfParser := parse.New(ednGrammar)

	for name, input := range inputs {
		b.Run("handrolled/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				edn.Parse(input)
			}
		})

		b.Run("ebnf/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ebnfParser.Parse(input, "edn")
			}
		})

		b.Run("adapter/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				node, _ := edn.Parse(input)
				tree := EDNToParseTree(node, input)
				parse.Transform(tree, testTransforms)
			}
		})
	}
}
