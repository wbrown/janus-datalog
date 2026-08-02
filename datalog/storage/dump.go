package storage

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

// IsDumpPath reports whether path names an exported dump rather than a store
// directory. The extension answers only that question: which of the two dump
// formats it holds is read from the dump itself, so a name and its contents
// cannot disagree.
func IsDumpPath(path string) bool {
	return strings.HasSuffix(path, ".edn") || strings.HasSuffix(path, ".jdzl")
}

// ImportDump loads an exported dump, taking the format from the dump's own
// bytes: a JDZL export opens with a 4-byte magic and anything else is EDN.
//
// r must be seekable because JDZL reads a trailer index and then seeks per
// chunk. os.File, the fs.File an embed.FS hands out, and bytes.Reader all
// qualify — which is what lets a database ship inside a binary.
func (d *Database) ImportDump(r io.ReadSeeker) error {
	var magic [len(binaryExportMagic)]byte
	switch _, err := io.ReadFull(r, magic[:]); {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		// Shorter than the magic, so not JDZL. An empty dump is an EDN dump of
		// no datoms, not an error.
	case err != nil:
		return fmt.Errorf("reading dump header: %w", err)
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewinding dump: %w", err)
	}
	if string(magic[:]) == binaryExportMagic {
		return d.ImportBinary(r)
	}
	return d.Import(r)
}
