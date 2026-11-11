package annotations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Both hands every event to both handlers, the first before the second; given
// one handler it answers that handler, and given none it answers none.
func TestBoth_HandsEveryEventToBothHandlersInOrder(t *testing.T) {
	var received []string
	first := func(e Event) { received = append(received, "first:"+e.Name) }
	second := func(e Event) { received = append(received, "second:"+e.Name) }

	both := Both(first, second)
	both(Event{Name: "a"})
	both(Event{Name: "b"})
	require.Equal(t, []string{"first:a", "second:a", "first:b", "second:b"}, received)

	received = nil
	Both(first, nil)(Event{Name: "c"})
	Both(nil, second)(Event{Name: "d"})
	require.Equal(t, []string{"first:c", "second:d"}, received, "a lone handler stands as given")
	require.Nil(t, Both(nil, nil), "no handler is no handler")
}
