package datalog

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBlobReferenceTaxonomyMatchesTheMinter ties the blob-reference taxonomy to
// what EncodeValue actually mints, in both directions.
//
// compressAndRoute is the only place a blob reference is created, and it returns
// the type tag and the BlobData together — so `blobData != nil` is the
// authoritative signal that a value took a blob, and the tag is derived from it
// rather than the other way round. Every assertion below draws its expectation
// from that signal.
//
// The bidirectional set comparison is the point. A tier added to EncodeValue but
// not to BlobReferenceTypes leaves a tag in minted and not in yielded; a tag
// listed in BlobReferenceTypes that nothing mints leaves the converse. Either way
// this fails, and a reclaimer that never asks about a live tag is what deletes a
// blob out from under the keys that reference it.
func TestBlobReferenceTaxonomyMatchesTheMinter(t *testing.T) {
	const threshold = 256

	// Both tier-3 cases need a compressed form above maxKeyValueSize, which
	// ordinary prose does not reach — it compresses into the key instead, landing
	// in tier 2 and minting no blob at all.
	printable := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,;:!?-_")
	text := make([]byte, 200000)
	raw := make([]byte, 200000)
	state := uint64(42)
	for i := range raw {
		state = state*6364136223846793005 + 1442695040888963407
		raw[i] = byte((state >> 33) & 0x7F)
		text[i] = printable[int(state>>33)%len(printable)]
	}
	highEntropyText := string(text)

	cases := []struct {
		name  string
		value Value
	}{
		{"tier-3 string", highEntropyText},
		{"tier-3 bytes", raw},
		{"tier-2 string", strings.Repeat("abcabcabc ", 200)},
		{"raw string", "short"},
		{"raw bytes", []byte("short")},
		{"int", int64(7)},
		{"float", 1.5},
		{"bool", true},
		{"time", time.Unix(1, 0).UTC()},
		{"keyword", NewKeyword(":a/b")},
		{"identity", NewIdentity("blob-taxonomy:entity")},
	}

	minted := map[ValueType]bool{}
	for _, testCase := range cases {
		vType, _, blobData := EncodeValue(testCase.value, threshold)
		require.Equal(t, blobData != nil, PayloadIsBlobReference(vType),
			"%s: EncodeValue produced %v with blobData!=nil=%v, classifier disagrees",
			testCase.name, vType, blobData != nil)
		if blobData != nil {
			minted[vType] = true
		}
	}
	require.NotEmpty(t, minted,
		"no case reached tier 3 — the table must mint a blob or this pins nothing")

	yielded := map[ValueType]bool{}
	for vType := range BlobReferenceTypes {
		yielded[vType] = true
	}
	require.Equal(t, minted, yielded,
		"BlobReferenceTypes must yield exactly the tags EncodeValue mints blobs under")
}

// TestPayloadIsBlobReferencePanicsOnUnclassifiedType pins the loud default. A
// value type added without a decision must detonate on first contact rather than
// read as "not blob-backed", which is the answer that deletes live blobs.
func TestPayloadIsBlobReferencePanicsOnUnclassifiedType(t *testing.T) {
	require.Panics(t, func() { PayloadIsBlobReference(ValueType(0xFE)) })
}
