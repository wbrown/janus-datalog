package queryfuzz

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A draw from one seed renders the same texts in the same order every time,
// so a test naming its seeds names its queries, and two seeds of the list
// render different texts.
func TestNewDraw_RendersTheSameTextsFromOneSeed(t *testing.T) {
	require.NotEmpty(t, Seeds, "the seed list names at least one seed")
	require.Greater(t, PerSeed, 0, "each seed draws at least one text")
	texts := func(seed int64) []string {
		d := NewDraw(seed)
		out := make([]string, 0, PerSeed)
		for i := 0; i < PerSeed; i++ {
			out = append(out, d.QueryText())
		}
		return out
	}
	for _, seed := range Seeds {
		require.Equal(t, texts(seed), texts(seed), "seed %d renders the same texts twice", seed)
	}
	require.NotEqual(t, texts(Seeds[0]), texts(Seeds[1]), "two seeds render different texts")
}
