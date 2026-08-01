package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// seekBuffer is an in-memory io.ReadWriteSeeker for binary export tests.
type seekBuffer struct {
	buf []byte
	off int64
}

func (s *seekBuffer) Write(p []byte) (int, error) {
	end := int(s.off) + len(p)
	if end > len(s.buf) {
		nb := make([]byte, end)
		copy(nb, s.buf)
		s.buf = nb
	}
	n := copy(s.buf[s.off:], p)
	s.off += int64(n)
	return n, nil
}

func (s *seekBuffer) Read(p []byte) (int, error) {
	if s.off >= int64(len(s.buf)) {
		return 0, io.EOF
	}
	n := copy(p, s.buf[s.off:])
	s.off += int64(n)
	return n, nil
}

func (s *seekBuffer) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = s.off + offset
	case io.SeekEnd:
		next = int64(len(s.buf)) + offset
	default:
		return 0, io.ErrUnexpectedEOF
	}
	if next < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	s.off = next
	return s.off, nil
}

func TestBinaryExportImport_RoundTrip(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db1 := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db1.NewTransaction()
			e1 := datalog.NewIdentity("bin:alice")
			e2 := datalog.NewIdentity("bin:bob")
			require.NoError(t, tx.Add(e1, datalog.NewKeyword(":person/name"), "Alice"))
			require.NoError(t, tx.Add(e1, datalog.NewKeyword(":person/age"), int64(30)))
			require.NoError(t, tx.Add(e1, datalog.NewKeyword(":person/active"), true))
			require.NoError(t, tx.Add(e1, datalog.NewKeyword(":person/created"), time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))
			require.NoError(t, tx.Add(e1, datalog.NewKeyword(":person/data"), []byte{1, 2, 3, 4}))
			require.NoError(t, tx.Add(e1, datalog.NewKeyword(":person/friend"), e2))
			require.NoError(t, tx.Add(e2, datalog.NewKeyword(":person/name"), "Bob"))
			_, err := tx.Commit()
			require.NoError(t, err)

			var out seekBuffer
			require.NoError(t, db1.ExportBinary(&out, BinaryExportOptions{SoftBudget: 64}))

			db2 := createOptimizerModeDB(t, mode, DatabaseOptions{})
			require.NoError(t, db2.ImportBinary(&out, BinaryImportOptions{Workers: 2}))

			var again seekBuffer
			require.NoError(t, db2.ExportBinary(&again, BinaryExportOptions{SoftBudget: 64}))
			require.Equal(t, out.buf, again.buf)
		})
	}
}

func TestBinaryExportImport_PreservesCRDTOps(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			sch, err := schema.NewBuilder().
				Attribute(":item/tags").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)

			db1 := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sch})

			e := datalog.NewIdentity("bin:crdt")
			tx := db1.NewTransaction()
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":item/tags"), "a"))
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":item/tags"), "b"))
			_, err = tx.Commit()
			require.NoError(t, err)

			tx2 := db1.NewTransaction()
			require.NoError(t, tx2.Remove(e, datalog.NewKeyword(":item/tags"), "a"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			var out seekBuffer
			require.NoError(t, db1.ExportBinary(&out))

			db2 := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sch})
			require.NoError(t, db2.ImportBinary(&out))

			tags, err := db2.GetStrings(e, datalog.NewKeyword(":item/tags"))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"b"}, tags)
		})
	}
}

func TestBinaryExport_EntityAlignedSoftClose(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			// Three entities; tiny soft budget forces closeSoon quickly, but each
			// entity's datoms must remain in one chunk.
			for _, name := range []string{"e0", "e1", "e2"} {
				e := datalog.NewIdentity("bin:align:" + name)
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":x/a"), name+"-a"))
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":x/b"), name+"-b"))
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":x/c"), name+"-c"))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			var out seekBuffer
			require.NoError(t, db.ExportBinary(&out, BinaryExportOptions{SoftBudget: 1}))

			hdr, err := readBinaryHeader(&out)
			require.NoError(t, err)
			trailer, err := readBinaryIndex(&out, hdr.indexOffset)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(trailer.entries), 1)

			for _, entry := range trailer.entries {
				require.Equal(t, entry.firstE, entry.lastE,
					"soft-close must not split an entity across chunks; first=%x last=%x", entry.firstE, entry.lastE)
			}
		})
	}
}

func TestBinaryExport_IndexSeekable(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			for i := 0; i < 5; i++ {
				e := datalog.NewIdentity("bin:idx:" + string(rune('a'+i)))
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":n/v"), int64(i)))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			var out seekBuffer
			require.NoError(t, db.ExportBinary(&out, BinaryExportOptions{SoftBudget: 1}))

			require.Equal(t, binaryExportMagic, string(out.buf[0:4]))
			indexOffset := binary.BigEndian.Uint64(out.buf[16:24])
			require.Greater(t, indexOffset, uint64(binaryHeaderSize))
			require.Equal(t, binaryIndexMagic, string(out.buf[indexOffset:indexOffset+4]))

			hdr, err := readBinaryHeader(&out)
			require.NoError(t, err)
			trailer, err := readBinaryIndex(&out, hdr.indexOffset)
			require.NoError(t, err)
			require.NotEmpty(t, trailer.entries)

			var seekMu sync.Mutex
			for _, entry := range trailer.entries {
				unc, err := readBinaryChunkPayload(&out, entry, &seekMu)
				require.NoError(t, err)
				datoms, err := decodeBinaryChunk(unc)
				require.NoError(t, err)
				require.NotEmpty(t, datoms)
				var first, last [20]byte
				copy(first[:], datoms[0].E.Bytes())
				copy(last[:], datoms[len(datoms)-1].E.Bytes())
				require.Equal(t, entry.firstE, first)
				require.Equal(t, entry.lastE, last)
			}
		})
	}
}

func TestBinaryImport_ParallelWorkers(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db1 := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db1.NewTransaction()
			for i := 0; i < 20; i++ {
				e := datalog.NewIdentity("bin:par:" + string(rune('a'+i%26)) + string(rune('0'+i/26)))
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":p/n"), int64(i)))
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":p/s"), "payload-"+string(rune('a'+i%26))))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			var out seekBuffer
			require.NoError(t, db1.ExportBinary(&out, BinaryExportOptions{SoftBudget: 32}))

			db2 := createOptimizerModeDB(t, mode, DatabaseOptions{})
			require.NoError(t, db2.ImportBinary(&out, BinaryImportOptions{Workers: 4}))

			var edn1, edn2 bytes.Buffer
			require.NoError(t, db1.Export(&edn1))
			require.NoError(t, db2.Export(&edn2))
			require.Equal(t, edn1.String(), edn2.String())
		})
	}
}

func TestBinaryExportImport_RGAAfterRef(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			sch, err := schema.NewBuilder().
				Attribute(":doc/items").Type(schema.TypeString).Vector().Add().
				Build()
			require.NoError(t, err)

			db1 := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sch})

			id := datalog.NewIdentity("bin:rga")
			items := datalog.NewKeyword(":doc/items")
			tx := db1.NewTransaction()
			require.NoError(t, tx.Add(id, items, "first"))
			require.NoError(t, tx.Add(id, items, "second"))
			require.NoError(t, tx.Add(id, items, "third"))
			_, err = tx.Commit()
			require.NoError(t, err)

			var out seekBuffer
			require.NoError(t, db1.ExportBinary(&out))

			// Decode every RGA record and require a real AfterRef chain: first insert
			// after HEAD (zero ElementID), later inserts after a prior insert's Tx.
			// Byte-identity alone would miss an encode that truncates AfterRef.
			hdr, err := readBinaryHeader(&out)
			require.NoError(t, err)
			trailer, err := readBinaryIndex(&out, hdr.indexOffset)
			require.NoError(t, err)
			var seekMu sync.Mutex
			type rgaRec struct {
				v        string
				tx       datalog.ElementID
				afterRef datalog.ElementID
			}
			var rga []rgaRec
			for _, entry := range trailer.entries {
				unc, err := readBinaryChunkPayload(&out, entry, &seekMu)
				require.NoError(t, err)
				datoms, err := decodeBinaryChunk(unc)
				require.NoError(t, err)
				for _, d := range datoms {
					if !d.Op.HasAfterRef() {
						continue
					}
					v, ok := d.V.(string)
					require.True(t, ok, "RGA value should be string, got %T", d.V)
					rga = append(rga, rgaRec{v: v, tx: d.Tx, afterRef: d.AfterRef})
				}
			}
			require.Len(t, rga, 3, "expected three RGA insert records")
			require.True(t, rga[0].afterRef.IsZero(), "first insert AfterRef should be HEAD (zero)")
			require.Equal(t, rga[0].tx, rga[1].afterRef, "second insert AfterRef must equal first Tx")
			require.Equal(t, rga[1].tx, rga[2].afterRef, "third insert AfterRef must equal second Tx")
			require.Equal(t, []string{"first", "second", "third"}, []string{rga[0].v, rga[1].v, rga[2].v})

			db2 := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sch})
			require.NoError(t, db2.ImportBinary(&out))

			type docItems struct {
				Items []string `datalog:"doc/items"`
			}
			var got docItems
			require.NoError(t, db2.PullInto(id, &got))
			require.Equal(t, []string{"first", "second", "third"}, got.Items)

			var again seekBuffer
			require.NoError(t, db2.ExportBinary(&again))
			require.Equal(t, out.buf, again.buf)
		})
	}
}

func TestDecodeBinaryChunk_TruncatedAfterRef(t *testing.T) {
	// Fixed prefix through flags (70 bytes), flag AfterRef, then only 8 of 16.
	unc := make([]byte, 70+8)
	unc[68] = byte(datalog.OpRGAInsert)
	unc[69] = binaryRecordFlagAfterRef
	_, err := decodeBinaryChunk(unc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "AfterRef")
}

func TestDecodeBinaryChunk_TruncatedValueHeaderAfterAfterRef(t *testing.T) {
	// Full AfterRef present, but cut before V_type / V_len — must not panic.
	unc := make([]byte, 70+16)
	unc[68] = byte(datalog.OpRGAInsert)
	unc[69] = binaryRecordFlagAfterRef
	_, err := decodeBinaryChunk(unc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "value header")
}

func TestBinaryImport_RejectsBadMagic(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("bin:bad"), datalog.NewKeyword(":t/v"), "x"))
			_, err := tx.Commit()
			require.NoError(t, err)

			var out seekBuffer
			require.NoError(t, db.ExportBinary(&out))
			out.buf[0] = 'X'

			db2 := createOptimizerModeDB(t, mode, DatabaseOptions{})
			err = db2.ImportBinary(&out)
			require.Error(t, err)
			require.Contains(t, err.Error(), "bad magic")
		})
	}
}

func TestBinaryImport_RejectsUnsupportedVersion(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("bin:ver"), datalog.NewKeyword(":t/v"), "x"))
			_, err := tx.Commit()
			require.NoError(t, err)

			var out seekBuffer
			require.NoError(t, db.ExportBinary(&out))
			out.buf[4] = binaryExportVersion + 1

			db2 := createOptimizerModeDB(t, mode, DatabaseOptions{})
			err = db2.ImportBinary(&out)
			require.Error(t, err)
			require.Contains(t, err.Error(), "unsupported version")
		})
	}
}

func TestBinaryImport_RejectsCorruptChunk(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db1 := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db1.NewTransaction()
			for i := 0; i < 8; i++ {
				e := datalog.NewIdentity("bin:corrupt:" + string(rune('a'+i)))
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":c/v"), int64(i)))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			var out seekBuffer
			require.NoError(t, db1.ExportBinary(&out, BinaryExportOptions{SoftBudget: 1}))

			hdr, err := readBinaryHeader(&out)
			require.NoError(t, err)
			trailer, err := readBinaryIndex(&out, hdr.indexOffset)
			require.NoError(t, err)
			require.NotEmpty(t, trailer.entries)

			// Corrupt the first chunk's type byte — reliable whether the payload is
			// LZJ-compressed or raw (small SoftBudget chunks often stay raw).
			entry := trailer.entries[0]
			require.Less(t, int(entry.offset), len(out.buf))
			out.buf[entry.offset] = 0xff

			db2 := createOptimizerModeDB(t, mode, DatabaseOptions{})
			err = db2.ImportBinary(&out, BinaryImportOptions{Workers: 4})
			require.Error(t, err)
			require.Contains(t, err.Error(), "unexpected chunk type")
		})
	}
}

func TestBinaryImport_RejectsOversizedIndexCount(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Minimal header + trailer claiming an absurd entry count for a tiny file.
			var buf seekBuffer
			require.NoError(t, writeBinaryHeader(&buf, 64, binaryHeaderSize))
			var hdr [24]byte
			copy(hdr[0:4], binaryIndexMagic)
			binary.BigEndian.PutUint32(hdr[4:8], math.MaxUint32)
			_, err := buf.Write(hdr[:])
			require.NoError(t, err)

			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			err = db.ImportBinary(&buf)
			require.Error(t, err)
			require.True(t,
				strings.Contains(err.Error(), "index count") || strings.Contains(err.Error(), "index"),
				"got: %v", err)
		})
	}
}

func TestBinaryUint32Len_RejectsOverflow(t *testing.T) {
	_, err := binaryUint32Len(-1, "value")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds uint32")

	// MaxUint32+1 is not representable as int on 32-bit hosts.
	if ^uint(0)>>32 == 0 {
		t.Skip("int cannot exceed MaxUint32 on this architecture")
	}
	n := int(uint64(math.MaxUint32) + 1)
	_, err = binaryUint32Len(n, "value")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds uint32")
}
