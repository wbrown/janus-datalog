package datalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ParseKeyword answers the keyword NewKeyword makes from the same text, the
// colon supplied when absent, and answers a KeywordTextError naming the text
// as the keyword would have been spelled for text that is no keyword's, in
// place of the panic NewKeyword raises.
func TestParseKeywordAnswersTheKeywordOrTheRefusal(t *testing.T) {
	for _, text := range []string{":t/parse-name", "t/parse-name"} {
		kw, err := ParseKeyword(text)
		require.NoError(t, err, "%q", text)
		require.Same(t, NewKeyword(":t/parse-name"), kw, "%q", text)
	}
	for _, c := range []struct{ text, refused string }{
		{"", ":"},
		{":", ":"},
		{"t/first name", ":t/first name"},
		{":x\ty", ":x\ty"},
	} {
		kw, err := ParseKeyword(c.text)
		require.Nil(t, kw, "%q", c.text)
		var refusal *KeywordTextError
		require.ErrorAs(t, err, &refusal, "%q", c.text)
		require.Equal(t, c.refused, refusal.Text, "%q", c.text)
	}
}

// ValidateKeywordText refuses text of one byte or less and text holding a
// space, and accepts every other text, as InternKeyword does where the
// keyword is made.
func TestValidateKeywordTextStatesTheRuleInternKeywordApplies(t *testing.T) {
	for _, text := range []string{"", ":", ":a b", ":x\ty"} {
		require.Error(t, ValidateKeywordText(text), "%q", text)
		require.Panics(t, func() { InternKeyword(text) }, "%q", text)
	}
	for _, text := range []string{":a", ":a.b/c-d"} {
		require.NoError(t, ValidateKeywordText(text), "%q", text)
		require.NotPanics(t, func() { InternKeyword(text) }, "%q", text)
	}
}
