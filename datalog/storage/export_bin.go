package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
)

const (
	binaryExportMagic        = "JDZL"
	binaryIndexMagic         = "JDZI"
	binaryExportVersion      = 1
	binaryHeaderSize         = 32
	binaryChunkHeaderSize    = 10
	binaryIndexEntrySize     = 56
	binaryChunkTypeData      = 1
	binaryChunkFlagRaw       = 0x01
	binaryRecordFlagAfterRef = 0x01
	defaultBinarySoftBudget  = 256 << 10
)

// BinaryExportOptions configures JDZL binary export.
type BinaryExportOptions struct {
	// SoftBudget is the advisory uncompressed chunk size in bytes. When the
	// open chunk reaches this size, export closes at the next entity boundary.
	// Zero selects the default (256KiB).
	SoftBudget int
	// SkipEntity, when non-nil, omits datoms whose entity returns true.
	SkipEntity func(datalog.Identity) bool
}

// BinaryImportOptions configures JDZL binary import.
type BinaryImportOptions struct {
	// Workers is the maximum number of chunks decoded and asserted concurrently.
	// Zero selects one: chunks then arrive in file order — the EAVT order the
	// tree store's bulk build takes as given — and the sweep in
	// BenchmarkDumpImportWorkers measured no throughput return on more.
	Workers int

	// Finalize runs once after the last chunk is written and the store's own
	// batch is complete, and not at all if any chunk failed. It is a hook for a
	// caller with its own work to close out; the store's batch is finished
	// whether or not one is given.
	Finalize func() error
}

type binaryIndexEntry struct {
	offset uint64
	cmpLen uint32
	uncLen uint32
	firstE [20]byte
	lastE  [20]byte
}

type binaryTrailer struct {
	entries []binaryIndexEntry
	maxTx   datalog.ElementID
}

// ExportBinary writes the complete EAVT datom log as a seekable JDZL file.
// w must be an io.WriteSeeker so the trailer index offset can be patched.
func (d *Database) ExportBinary(w io.WriteSeeker, opts ...BinaryExportOptions) error {
	softBudget := defaultBinarySoftBudget
	var skipEntity func(datalog.Identity) bool
	if len(opts) > 0 {
		if opts[0].SoftBudget > 0 {
			softBudget = opts[0].SoftBudget
		}
		skipEntity = opts[0].SkipEntity
	}

	if _, err := w.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("binary export seek start: %w", err)
	}
	if err := writeBinaryHeader(w, softBudget, 0); err != nil {
		return err
	}

	iter, err := OpenScan(d.store, DiscardIntake, ScanBound{Index: EAVT})
	if err != nil {
		return fmt.Errorf("binary export scan: %w", err)
	}
	defer iter.Close()

	var (
		trailer    binaryTrailer
		chunk      []byte
		openE      [20]byte
		haveOpenE  bool
		closeSoon  bool
		firstE     [20]byte
		lastE      [20]byte
		chunkOpen  bool
		fileOffset int64 = binaryHeaderSize
	)

	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		entry, written, err := writeBinaryChunk(w, uint64(fileOffset), chunk, firstE, lastE)
		if err != nil {
			return err
		}
		trailer.entries = append(trailer.entries, entry)
		fileOffset += written
		chunk = chunk[:0]
		haveOpenE = false
		closeSoon = false
		chunkOpen = false
		return nil
	}

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return fmt.Errorf("binary export datom: %w", err)
		}
		if skipEntity != nil && skipEntity(datom.E) {
			continue
		}

		var eBytes [20]byte
		copy(eBytes[:], datom.E.Bytes())

		// Soft budget arms closeSoon; the chunk actually closes on the next
		// entity boundary so an entity's datoms stay in one LZJ window.
		if haveOpenE && eBytes != openE && closeSoon {
			if err := flush(); err != nil {
				return err
			}
		}

		rec, err := encodeBinaryDatom(datom)
		if err != nil {
			return err
		}

		if !haveOpenE {
			openE = eBytes
			haveOpenE = true
		} else if eBytes != openE {
			openE = eBytes
		}
		if !chunkOpen {
			firstE = eBytes
			chunkOpen = true
		}
		lastE = eBytes

		chunk = append(chunk, rec...)
		if !datom.Tx.IsZero() && (trailer.maxTx.IsZero() || trailer.maxTx.Less(datom.Tx)) {
			trailer.maxTx = datom.Tx
		}
		if len(chunk) >= softBudget {
			closeSoon = true
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("binary export scan: %w", err)
	}
	if err := flush(); err != nil {
		return err
	}

	indexOffset := uint64(fileOffset)
	if err := writeBinaryIndex(w, trailer); err != nil {
		return err
	}
	if _, err := w.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("binary export patch header: %w", err)
	}
	return writeBinaryHeader(w, softBudget, indexOffset)
}

// ImportBinary reads a JDZL file and asserts its datoms. r must be an
// io.ReadSeeker. Chunks are decoded and asserted in parallel up to Workers.
//
// The first worker error cancels remaining launches and in-flight work between
// steps, then that error is returned. Import is not transactional across
// chunks: workers that already Asserted leave a nondeterministic partial
// subset of the file in the store. Retrying into the same database is not a
// safe recovery; use a fresh database (or discard the target) after failure.
func (d *Database) ImportBinary(r io.ReadSeeker, opts ...BinaryImportOptions) error {
	workers := 1
	var finalize func() error
	if len(opts) > 0 {
		if opts[0].Workers > 0 {
			workers = opts[0].Workers
		}
		finalize = opts[0].Finalize
	}

	header, err := readBinaryHeader(r)
	if err != nil {
		return err
	}
	trailer, err := readBinaryIndex(r, header.indexOffset)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var seekMu sync.Mutex
	sem := make(chan struct{}, workers)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	var reported atomic.Bool
	reportErr := func(err error) {
		if err == nil || !reported.CompareAndSwap(false, true) {
			return
		}
		cancel()
		errCh <- err
	}

	for i := range trailer.entries {
		entry := trailer.entries[i]
		select {
		case <-ctx.Done():
			// Stop launching; in-flight workers still finish or exit on ctx checks.
		case sem <- struct{}{}:
			wg.Add(1)
			go func(entry binaryIndexEntry) {
				defer wg.Done()
				defer func() { <-sem }()
				if ctx.Err() != nil {
					return
				}
				unc, err := readBinaryChunkPayload(r, entry, &seekMu)
				if err != nil {
					reportErr(err)
					return
				}
				if ctx.Err() != nil {
					return
				}
				if len(unc) == 0 || ctx.Err() != nil {
					return
				}
				if err := assertBinaryChunk(d.store, unc); err != nil {
					reportErr(fmt.Errorf("binary import assert chunk at %d: %w", entry.offset, err))
				}
			}(entry)
			continue
		}
		break
	}
	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		return err
	}

	// Every chunk is written, so a store holding its write open across them has
	// the whole dump and nothing further is coming. This runs before the clock is
	// restored: the datoms the restored ElementID describes must already be in
	// the store.
	if err := d.store.FinishBatch(); err != nil {
		return fmt.Errorf("binary import finish batch: %w", err)
	}
	if finalize != nil {
		if err := finalize(); err != nil {
			return fmt.Errorf("binary import finalize: %w", err)
		}
	}

	maxElementID := trailer.maxTx
	if maxElementID.IsZero() {
		maxElementID, err = d.store.MaxElementID()
		if err != nil {
			return fmt.Errorf("binary import max ElementID: %w", err)
		}
	}
	if !maxElementID.IsZero() {
		d.clock.Restore(maxElementID)
	}
	return nil
}

func writeBinaryHeader(w io.Writer, softBudget int, indexOffset uint64) error {
	var hdr [binaryHeaderSize]byte
	copy(hdr[0:4], binaryExportMagic)
	hdr[4] = binaryExportVersion
	binary.BigEndian.PutUint32(hdr[8:12], uint32(softBudget))
	binary.BigEndian.PutUint64(hdr[16:24], indexOffset)
	_, err := w.Write(hdr[:])
	return err
}

type binaryFileHeader struct {
	softBudget  uint32
	indexOffset uint64
}

func readBinaryHeader(r io.ReadSeeker) (binaryFileHeader, error) {
	var hdr [binaryHeaderSize]byte
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return binaryFileHeader{}, err
	}
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return binaryFileHeader{}, fmt.Errorf("binary import header: %w", err)
	}
	if string(hdr[0:4]) != binaryExportMagic {
		return binaryFileHeader{}, fmt.Errorf("binary import: bad magic %q", hdr[0:4])
	}
	if hdr[4] != binaryExportVersion {
		return binaryFileHeader{}, fmt.Errorf("binary import: unsupported version %d", hdr[4])
	}
	return binaryFileHeader{
		softBudget:  binary.BigEndian.Uint32(hdr[8:12]),
		indexOffset: binary.BigEndian.Uint64(hdr[16:24]),
	}, nil
}

func writeBinaryChunk(w io.Writer, offset uint64, unc []byte, firstE, lastE [20]byte) (binaryIndexEntry, int64, error) {
	flags := byte(0)
	payload := codec.Compress(unc)
	if payload == nil {
		flags = binaryChunkFlagRaw
		payload = unc
	}
	uncLen, err := binaryUint32Len(len(unc), "uncompressed chunk")
	if err != nil {
		return binaryIndexEntry{}, 0, err
	}
	cmpLen, err := binaryUint32Len(len(payload), "compressed chunk")
	if err != nil {
		return binaryIndexEntry{}, 0, err
	}
	var hdr [binaryChunkHeaderSize]byte
	hdr[0] = binaryChunkTypeData
	hdr[1] = flags
	binary.BigEndian.PutUint32(hdr[2:6], uncLen)
	binary.BigEndian.PutUint32(hdr[6:10], cmpLen)
	n, err := w.Write(hdr[:])
	if err != nil {
		return binaryIndexEntry{}, 0, err
	}
	m, err := w.Write(payload)
	if err != nil {
		return binaryIndexEntry{}, 0, err
	}
	return binaryIndexEntry{
		offset: offset,
		cmpLen: cmpLen,
		uncLen: uncLen,
		firstE: firstE,
		lastE:  lastE,
	}, int64(n + m), nil
}

func writeBinaryIndex(w io.Writer, trailer binaryTrailer) error {
	count, err := binaryUint32Len(len(trailer.entries), "index entry count")
	if err != nil {
		return err
	}
	var hdr [24]byte
	copy(hdr[0:4], binaryIndexMagic)
	binary.BigEndian.PutUint32(hdr[4:8], count)
	binary.BigEndian.PutUint64(hdr[8:16], trailer.maxTx.Lamport)
	binary.BigEndian.PutUint64(hdr[16:24], trailer.maxTx.ReplicaID)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	var entry [binaryIndexEntrySize]byte
	for _, e := range trailer.entries {
		binary.BigEndian.PutUint64(entry[0:8], e.offset)
		binary.BigEndian.PutUint32(entry[8:12], e.cmpLen)
		binary.BigEndian.PutUint32(entry[12:16], e.uncLen)
		copy(entry[16:36], e.firstE[:])
		copy(entry[36:56], e.lastE[:])
		if _, err := w.Write(entry[:]); err != nil {
			return err
		}
	}
	return nil
}

func readBinaryIndex(r io.ReadSeeker, indexOffset uint64) (binaryTrailer, error) {
	if indexOffset < binaryHeaderSize {
		return binaryTrailer{}, errors.New("binary import: invalid index offset")
	}
	fileSize, err := seekerSize(r)
	if err != nil {
		return binaryTrailer{}, err
	}
	if indexOffset > uint64(fileSize) {
		return binaryTrailer{}, errors.New("binary import: index offset past end of file")
	}
	if _, err := r.Seek(int64(indexOffset), io.SeekStart); err != nil {
		return binaryTrailer{}, fmt.Errorf("binary import seek index: %w", err)
	}
	var hdr [24]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return binaryTrailer{}, fmt.Errorf("binary import index header: %w", err)
	}
	if string(hdr[0:4]) != binaryIndexMagic {
		return binaryTrailer{}, fmt.Errorf("binary import: bad index magic %q", hdr[0:4])
	}
	count := binary.BigEndian.Uint32(hdr[4:8])
	remaining := uint64(fileSize) - indexOffset - uint64(len(hdr))
	maxEntries := remaining / binaryIndexEntrySize
	if uint64(count) > maxEntries {
		return binaryTrailer{}, fmt.Errorf("binary import: index count %d exceeds remaining file capacity %d", count, maxEntries)
	}
	trailer := binaryTrailer{
		entries: make([]binaryIndexEntry, 0, count),
		maxTx: datalog.ElementID{
			Lamport:   binary.BigEndian.Uint64(hdr[8:16]),
			ReplicaID: binary.BigEndian.Uint64(hdr[16:24]),
		},
	}
	var entry [binaryIndexEntrySize]byte
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, entry[:]); err != nil {
			return binaryTrailer{}, fmt.Errorf("binary import index entry %d: %w", i, err)
		}
		var e binaryIndexEntry
		e.offset = binary.BigEndian.Uint64(entry[0:8])
		e.cmpLen = binary.BigEndian.Uint32(entry[8:12])
		e.uncLen = binary.BigEndian.Uint32(entry[12:16])
		copy(e.firstE[:], entry[16:36])
		copy(e.lastE[:], entry[36:56])
		trailer.entries = append(trailer.entries, e)
	}
	return trailer, nil
}

func readBinaryChunkPayload(r io.ReadSeeker, entry binaryIndexEntry, seekMu *sync.Mutex) ([]byte, error) {
	payload, flags, err := readBinaryChunkBytes(r, entry, seekMu)
	if err != nil {
		return nil, err
	}
	if flags&binaryChunkFlagRaw != 0 {
		if uint32(len(payload)) != entry.uncLen {
			return nil, fmt.Errorf("binary import: raw chunk size mismatch")
		}
		return payload, nil
	}
	unc, err := codec.Decompress(payload)
	if err != nil {
		return nil, fmt.Errorf("binary import decompress: %w", err)
	}
	if uint32(len(unc)) != entry.uncLen {
		return nil, fmt.Errorf("binary import: decompressed length mismatch")
	}
	return unc, nil
}

// readBinaryChunkBytes reads one chunk's stored bytes and its flags. The lock
// covers the seek and the read, which share the file; decompression shares
// nothing and runs outside it.
func readBinaryChunkBytes(r io.ReadSeeker, entry binaryIndexEntry, seekMu *sync.Mutex) ([]byte, byte, error) {
	seekMu.Lock()
	defer seekMu.Unlock()
	fileSize, err := seekerSize(r)
	if err != nil {
		return nil, 0, err
	}
	if entry.offset > uint64(fileSize) {
		return nil, 0, fmt.Errorf("binary import: chunk offset %d past end of file", entry.offset)
	}
	if _, err := r.Seek(int64(entry.offset), io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("binary import seek chunk: %w", err)
	}
	var hdr [binaryChunkHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, 0, fmt.Errorf("binary import chunk header: %w", err)
	}
	if hdr[0] != binaryChunkTypeData {
		return nil, 0, fmt.Errorf("binary import: unexpected chunk type %d", hdr[0])
	}
	flags := hdr[1]
	uncLen := binary.BigEndian.Uint32(hdr[2:6])
	cmpLen := binary.BigEndian.Uint32(hdr[6:10])
	if uncLen != entry.uncLen || cmpLen != entry.cmpLen {
		return nil, 0, fmt.Errorf("binary import: chunk length mismatch at %d", entry.offset)
	}
	payloadEnd := entry.offset + uint64(binaryChunkHeaderSize) + uint64(cmpLen)
	if payloadEnd > uint64(fileSize) {
		return nil, 0, fmt.Errorf("binary import: chunk payload at %d exceeds file size", entry.offset)
	}
	payload := make([]byte, cmpLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, fmt.Errorf("binary import chunk payload: %w", err)
	}
	return payload, flags, nil
}

func encodeBinaryDatom(d *datalog.Datom) ([]byte, error) {
	if d.E == nil {
		return nil, errors.New("binary export: nil entity")
	}
	if d.A == nil {
		return nil, errors.New("binary export: nil attribute")
	}
	if len(d.A.String()) > datalog.MaxAttributeBytes {
		return nil, fmt.Errorf("binary export: attribute %q exceeds %d bytes", d.A.String(), datalog.MaxAttributeBytes)
	}

	vBytes := datalog.ValueBytes(d.V)
	vLen, err := binaryUint32Len(len(vBytes), "value")
	if err != nil {
		return nil, err
	}
	recLen := 20 + 32 + 16 + 1 + 1 + 1 + 4 + len(vBytes)
	if d.Op.HasAfterRef() {
		recLen += 16
	}
	buf := make([]byte, recLen)
	off := 0
	copy(buf[off:off+20], d.E.Bytes())
	off += 20
	copy(buf[off:off+32], d.A.String())
	off += 32
	copy(buf[off:off+16], d.Tx.Bytes())
	off += 16
	buf[off] = byte(d.Op)
	off++
	flags := byte(0)
	if d.Op.HasAfterRef() {
		flags |= binaryRecordFlagAfterRef
	}
	buf[off] = flags
	off++
	if flags&binaryRecordFlagAfterRef != 0 {
		copy(buf[off:off+16], d.AfterRef.Bytes())
		off += 16
	}
	buf[off] = byte(datalog.Type(d.V))
	off++
	binary.BigEndian.PutUint32(buf[off:off+4], vLen)
	off += 4
	copy(buf[off:], vBytes)
	return buf, nil
}

// assertBinaryChunk decodes one chunk straight into the store: each record goes
// from the cursor to the store's add as it is read, so the chunk never becomes a
// slice of datoms. JDZL closes a chunk only at an entity boundary, so one entity
// is the floor and a chunk has no size ceiling — that slice was the unbounded
// one.
//
// What arrival means is the backend's: the tree store inserts the datom into the
// version it is building, Badger encodes it into a write batch.
func assertBinaryChunk(store Store, unc []byte) error {
	return store.AssertEach(func(add func(*datalog.Datom) error) error {
		cursor := newBinaryChunkCursor(unc)
		for cursor.Next() {
			if err := add(cursor.Datom()); err != nil {
				return err
			}
		}
		return cursor.Err()
	})
}

// binaryChunkCursor walks a chunk one record at a time.
//
// Records are fixed-width or self-delimiting and carry no cross-record state,
// so a chunk never has to become a slice of datoms — and it must not: JDZL
// closes a chunk only at an entity boundary, so one entity is the floor and a
// chunk has no size ceiling.
//
// Datom is the cursor's workspace, valid until the next Next. Values alias unc,
// so unc must outlive the walk.
type binaryChunkCursor struct {
	unc []byte
	off int
	d   datalog.Datom
	err error
}

func newBinaryChunkCursor(unc []byte) *binaryChunkCursor {
	return &binaryChunkCursor{unc: unc}
}

// Datom returns the record Next just decoded.
func (c *binaryChunkCursor) Datom() *datalog.Datom { return &c.d }

// Err reports why iteration stopped early. Nil after a walk that ran out of
// records normally.
func (c *binaryChunkCursor) Err() error { return c.err }

func (c *binaryChunkCursor) Next() bool {
	if c.err != nil || c.off >= len(c.unc) {
		return false
	}
	d, off, err := decodeBinaryRecord(c.unc, c.off)
	if err != nil {
		c.err = err
		return false
	}
	c.d, c.off = d, off
	return true
}

// decodeBinaryRecord decodes the record at off and returns the offset of the
// next one.
func decodeBinaryRecord(unc []byte, off int) (datalog.Datom, int, error) {
	var zero datalog.Datom

	// Fixed prefix through flags; AfterRef and value header checked after.
	const prefixLen = 20 + 32 + 16 + 1 + 1
	if off+prefixLen > len(unc) {
		return zero, 0, fmt.Errorf("binary import: truncated record at %d", off)
	}
	var eHash [20]byte
	copy(eHash[:], unc[off:off+20])
	off += 20
	var aBytes [32]byte
	copy(aBytes[:], unc[off:off+32])
	off += 32
	tx := datalog.ElementIDFromBytes(unc[off : off+16])
	off += 16
	op := datalog.CRDTOp(unc[off])
	off++
	flags := unc[off]
	off++
	var afterRef datalog.ElementID
	if flags&binaryRecordFlagAfterRef != 0 {
		if off+16 > len(unc) {
			return zero, 0, fmt.Errorf("binary import: truncated AfterRef at %d", off)
		}
		afterRef = datalog.ElementIDFromBytes(unc[off : off+16])
		off += 16
	}
	if off+5 > len(unc) {
		return zero, 0, fmt.Errorf("binary import: truncated value header at %d", off)
	}
	vType := datalog.ValueType(unc[off])
	off++
	vLen := binary.BigEndian.Uint32(unc[off : off+4])
	off += 4
	if uint64(off)+uint64(vLen) > uint64(len(unc)) {
		return zero, 0, fmt.Errorf("binary import: truncated value at %d", off)
	}
	v, err := datalog.ValueFromBytes(vType, unc[off:off+int(vLen)])
	if err != nil {
		return zero, 0, fmt.Errorf("binary import value: %w", err)
	}
	off += int(vLen)

	return datalog.Datom{
		E:        datalog.NewIdentityFromHash(eHash),
		A:        datalog.InternKeywordFromBytes(aBytes),
		V:        v,
		Tx:       tx,
		Op:       op,
		AfterRef: afterRef,
	}, off, nil
}

func binaryUint32Len(n int, what string) (uint32, error) {
	// Compare in uint64 space so this compiles when int is 32-bit
	// (n > math.MaxUint32 is not a valid int comparison on 32-bit).
	if n < 0 || uint64(n) > math.MaxUint32 {
		return 0, fmt.Errorf("binary export: %s length %d exceeds uint32", what, n)
	}
	return uint32(n), nil
}

func seekerSize(r io.ReadSeeker) (int64, error) {
	cur, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("binary import tell: %w", err)
	}
	end, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("binary import size: %w", err)
	}
	if _, err := r.Seek(cur, io.SeekStart); err != nil {
		return 0, fmt.Errorf("binary import restore: %w", err)
	}
	return end, nil
}
