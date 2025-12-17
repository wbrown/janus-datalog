package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// kw helper is defined in schema_test.go

func TestResolvePullPatternNilPattern(t *testing.T) {
	schema, _ := NewBuilder().
		Attribute(":person/name").Type(TypeString).Add().
		Build()

	resolved := ResolvePullPattern(nil, schema)
	assert.Nil(t, resolved)
}

func TestResolvePullPatternNilSchema(t *testing.T) {
	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: kw(":person/name")},
			&query.PullAttribute{Attr: kw(":person/friends")},
		},
	}

	// With nil schema, all attributes default to cardinality-one, not ref
	resolved := ResolvePullPattern(pattern, nil)
	require.NotNil(t, resolved)
	require.Len(t, resolved.Specs, 2)

	nameSpec := resolved.Specs[0].(*query.ResolvedPullAttribute)
	assert.Equal(t, kw(":person/name"), nameSpec.Attr)
	assert.False(t, nameSpec.IsMany)
	assert.False(t, nameSpec.IsRef)

	friendsSpec := resolved.Specs[1].(*query.ResolvedPullAttribute)
	assert.Equal(t, kw(":person/friends"), friendsSpec.Attr)
	assert.False(t, friendsSpec.IsMany)
	assert.False(t, friendsSpec.IsRef)
}

func TestResolvePullPatternWithSchema(t *testing.T) {
	schema, _ := NewBuilder().
		Attribute(":person/name").Type(TypeString).Add().
		Attribute(":person/friends").Type(TypeRef).Many().Add().
		Attribute(":person/manager").Type(TypeRef).Add().
		Build()

	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: kw(":person/name")},
			&query.PullAttribute{Attr: kw(":person/friends")},
			&query.PullAttribute{Attr: kw(":person/manager")},
			&query.PullAttribute{Attr: kw(":person/unknown")}, // Not in schema
		},
	}

	resolved := ResolvePullPattern(pattern, schema)
	require.NotNil(t, resolved)
	require.Len(t, resolved.Specs, 4)

	// :person/name - string, cardinality-one
	nameSpec := resolved.Specs[0].(*query.ResolvedPullAttribute)
	assert.False(t, nameSpec.IsMany)
	assert.False(t, nameSpec.IsRef)

	// :person/friends - ref, cardinality-many
	friendsSpec := resolved.Specs[1].(*query.ResolvedPullAttribute)
	assert.True(t, friendsSpec.IsMany)
	assert.True(t, friendsSpec.IsRef)

	// :person/manager - ref, cardinality-one
	managerSpec := resolved.Specs[2].(*query.ResolvedPullAttribute)
	assert.False(t, managerSpec.IsMany)
	assert.True(t, managerSpec.IsRef)

	// :person/unknown - not in schema, defaults to one/not-ref
	unknownSpec := resolved.Specs[3].(*query.ResolvedPullAttribute)
	assert.False(t, unknownSpec.IsMany)
	assert.False(t, unknownSpec.IsRef)
}

func TestResolvePullPatternWildcard(t *testing.T) {
	schema, _ := NewBuilder().
		Attribute(":person/name").Type(TypeString).Add().
		Build()

	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullWildcard{},
		},
	}

	resolved := ResolvePullPattern(pattern, schema)
	require.NotNil(t, resolved)
	require.Len(t, resolved.Specs, 1)

	_, ok := resolved.Specs[0].(*query.ResolvedPullWildcard)
	assert.True(t, ok)
}

func TestResolvePullPatternMapSpec(t *testing.T) {
	schema, _ := NewBuilder().
		Attribute(":person/friends").Type(TypeRef).Many().Add().
		Attribute(":person/name").Type(TypeString).Add().
		Build()

	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullMapSpec{
				Attr: kw(":person/friends"),
				Pattern: &query.PullPattern{
					Specs: []query.PullAttrSpec{
						&query.PullAttribute{Attr: kw(":person/name")},
					},
				},
			},
		},
	}

	resolved := ResolvePullPattern(pattern, schema)
	require.NotNil(t, resolved)
	require.Len(t, resolved.Specs, 1)

	mapSpec := resolved.Specs[0].(*query.ResolvedPullMapSpec)
	assert.Equal(t, kw(":person/friends"), mapSpec.Attr)
	assert.True(t, mapSpec.IsMany) // friends is cardinality-many

	// Nested pattern should also be resolved
	require.NotNil(t, mapSpec.Pattern)
	require.Len(t, mapSpec.Pattern.Specs, 1)
	nestedAttr := mapSpec.Pattern.Specs[0].(*query.ResolvedPullAttribute)
	assert.Equal(t, kw(":person/name"), nestedAttr.Attr)
	assert.False(t, nestedAttr.IsMany)
	assert.False(t, nestedAttr.IsRef)
}

func TestResolvePullPatternLimitExpr(t *testing.T) {
	schema, _ := NewBuilder().
		Attribute(":person/tags").Type(TypeString).Many().Add().
		Build()

	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullLimitExpr{Attr: kw(":person/tags"), Limit: 5},
		},
	}

	resolved := ResolvePullPattern(pattern, schema)
	require.NotNil(t, resolved)
	require.Len(t, resolved.Specs, 1)

	limitSpec := resolved.Specs[0].(*query.ResolvedPullLimitExpr)
	assert.Equal(t, kw(":person/tags"), limitSpec.Attr)
	assert.Equal(t, 5, limitSpec.Limit)
	assert.True(t, limitSpec.IsMany)
	assert.False(t, limitSpec.IsRef)
}

func TestResolvePullPatternDefaultExpr(t *testing.T) {
	schema, _ := NewBuilder().
		Attribute(":person/status").Type(TypeString).Add().
		Build()

	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullDefaultExpr{Attr: kw(":person/status"), Default: "unknown"},
		},
	}

	resolved := ResolvePullPattern(pattern, schema)
	require.NotNil(t, resolved)
	require.Len(t, resolved.Specs, 1)

	defaultSpec := resolved.Specs[0].(*query.ResolvedPullDefaultExpr)
	assert.Equal(t, kw(":person/status"), defaultSpec.Attr)
	assert.Equal(t, "unknown", defaultSpec.Default)
	assert.False(t, defaultSpec.IsMany)
	assert.False(t, defaultSpec.IsRef)
}

func TestResolvePullPatternString(t *testing.T) {
	schema, _ := NewBuilder().
		Attribute(":person/friends").Type(TypeRef).Many().Add().
		Build()

	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: kw(":person/name")},
			&query.PullAttribute{Attr: kw(":person/friends")},
		},
	}

	resolved := ResolvePullPattern(pattern, schema)
	str := resolved.String()

	// String representation should show cardinality info
	assert.Contains(t, str, ":person/name")
	assert.Contains(t, str, ":person/friends[many][ref]")
}
