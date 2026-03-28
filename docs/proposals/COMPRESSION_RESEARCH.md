# Compression Research: Algorithms and Order-Preserving Techniques

Reference material for the [Compressed String Values](COMPRESSED_STRING_VALUES.md) proposal.
Research conducted 2026-03-28.

---

## Part 1: zstd Internals and Minimal Implementation

### 1. Core Algorithmic Components of zstd

zstd's compression pipeline has three main stages:

#### Stage 1: LZ77-style Match Finding

zstd uses LZ77 as its foundation, same as DEFLATE. It finds repeated byte sequences and encodes them as (literal-run, match-offset, match-length) triples. The key differences from DEFLATE's LZ77:

- **Larger window sizes**: DEFLATE is limited to 32KB windows. zstd supports up to 128MB windows (and even larger with long-distance matching). This is the single biggest contributor to better compression on large files — matches that DEFLATE literally cannot see because they're too far back.

- **Repeat offsets**: zstd maintains a buffer of the 3 most recently used offsets. When a match reuses a recent offset, it encodes as offset 1, 2, or 3 instead of the full offset value. This is extremely effective on structured data where the same field spacing recurs (JSON keys at regular intervals, struct fields, etc.). DEFLATE has no equivalent.

- **Sequence encoding**: Rather than interleaving literals and matches in a single stream, zstd separates them into three parallel streams: literals lengths, match lengths, and offsets. Each stream can be compressed independently with its own entropy coder.

#### Stage 2: Huffman Coding for Literals

Raw literal bytes (the parts that didn't match anything) are compressed with Huffman coding. This is the same approach as DEFLATE, though zstd can optionally use multiple Huffman tables per block or skip Huffman entirely if literals are high-entropy.

#### Stage 3: FSE (Finite State Entropy) for Sequences

The three sequence streams (literal-lengths, match-lengths, offsets) are each encoded with FSE (Finite State Entropy), which is Yann Collet's implementation of tANS (table-based Asymmetric Numeral Systems).

**This is the key innovation over DEFLATE's Huffman coding for sequences.** FSE achieves near-Shannon-entropy compression even for skewed distributions, while Huffman is limited to integer-bit codes (minimum 1 bit per symbol).

#### The Sequence Encoding Pipeline in Detail

A zstd compressed block contains:
1. **Literals section**: Raw bytes compressed with Huffman (or stored raw/RLE)
2. **Sequences section**: Three interleaved FSE-coded streams
   - Literal lengths (how many raw bytes before each match)
   - Offsets (where the match starts, relative to current position)
   - Match lengths (how long the match is)
3. **FSE tables**: Distribution tables for each stream (can be predefined, shared, or per-block)

Each value in the sequence streams is encoded as a **baseline + extra bits** pair. For example, a match length of 35 might encode as code 25 (baseline 35, 0 extra bits). Larger values use more extra bits. This is similar to DEFLATE's approach but with finer granularity and FSE instead of Huffman for the code symbols.

### 2. What Contributes Most to Compression Ratio Improvement Over DEFLATE

Ranked by impact:

#### (a) Larger match windows — HIGH impact on large files

DEFLATE's 32KB window means it misses long-distance repetitions entirely. On files larger than ~64KB, zstd's larger window finds matches DEFLATE cannot. On small files (<32KB), this advantage vanishes.

**Quantified**: On the Silesia benchmark corpus, increasing window from 32KB to 1MB alone accounts for roughly 5-15% better compression on text files. Going to 8MB+ adds another few percent on larger files.

#### (b) FSE/tANS entropy coding — MODERATE impact

FSE codes fractional bits. Huffman cannot code a symbol at less than 1 bit. For highly skewed distributions (e.g., offset-code distribution where offset 1 occurs 40% of the time), FSE saves 5-15% over Huffman on the sequence streams.

On text data specifically, the sequence distributions tend to be moderately skewed (common match lengths, common small offsets), so FSE typically saves 3-8% over what Huffman would achieve for the same data.

**However**: The sequences are a minority of the compressed output. Literals dominate on text. FSE's advantage applies mainly to the ~30-40% of output that encodes sequences. Net improvement on total output: roughly 2-5%.

#### (c) Repeat offsets — MODERATE impact on structured data

On JSON/EDN/structured text, repeat offsets are very effective. Field names recur at regular intervals. Encoding "same offset as last time" costs ~1 bit vs 15-25 bits for a full offset.

**Quantified**: On JSON data, repeat offsets capture 20-40% of all matches. On natural language prose, it's more like 10-20%. Net compression improvement: 3-8% on structured data, 1-3% on prose.

#### (d) Separated streams — LOW-MODERATE impact

Separating literals, literal-lengths, match-lengths, and offsets into independent streams lets each have its own optimized entropy table. DEFLATE interleaves everything into one Huffman table shared between literals and length/distance codes.

This helps because the statistical distributions are fundamentally different between these categories. Impact: ~1-3%.

#### (e) Better match finder heuristics — LOW impact on ratio, HIGH on speed

zstd's match finders (hash chains, binary trees, optimal parsing at high levels) are more sophisticated than typical DEFLATE implementations. At comparable speed, zstd finds slightly better matches. At high compression levels, zstd's optimal parser can explore much larger match-space.

**Summary**: On text data, the rough breakdown of zstd's advantage over DEFLATE is:
- Larger windows: ~40% of the improvement
- FSE for sequences: ~20% of the improvement
- Repeat offsets: ~20% of the improvement
- Stream separation: ~10% of the improvement
- Better match finding: ~10% of the improvement

### 3. What's in the zstd Format That You Wouldn't Need in a Custom Format

#### The zstd frame format includes:

**Framing overhead (not needed for custom use):**
- Magic number (4 bytes) — format identification
- Frame header with flags for: window size, single-segment flag, content size, checksum flag, dictionary ID
- Block headers (3 bytes each: last-block flag, block type, block size)
- Optional content checksum (xxHash64, 4 bytes)
- Skippable frames (for metadata embedding)

**Dictionary support (not needed without pre-shared dictionaries):**
- Dictionary ID field in frame header
- Dictionary decoding tables (Huffman + 3 FSE tables)
- Dictionary content (raw bytes for initial match window)
- Dictionary is specifically for small data (<1KB) where there isn't enough data to build good statistics

**Streaming/incremental features (not needed for single-shot compression):**
- Multi-block support within a frame
- Block-level types (raw, RLE, compressed, reserved)
- The ability to reset statistics per-block vs reusing previous block's tables

**Backward compatibility / generality:**
- Multiple Huffman table transmission formats (direct, FSE-compressed weights)
- Predefined FSE distribution tables (for when explicit tables would cost more than they save)
- The "repeat" mode for FSE tables (reuse previous block's table)
- RLE blocks (entire block is one repeated byte)
- Raw blocks (store uncompressed when compression would expand)

#### What you'd actually need for a minimal custom format:

1. LZ77 match finding with configurable window
2. Literal encoding (Huffman or raw)
3. Sequence encoding: literal-lengths, offsets, match-lengths with FSE
4. FSE table serialization (just one format, not three alternatives)
5. Repeat offset tracking (3 recent offsets)
6. A minimal block/stream header (just enough to know sizes)

You could skip: magic numbers, checksums, dictionary support, multiple block types, skippable frames, predefined tables, the repeat-table mode, and the elaborate flag-based header.

### 4. Complexity of FSE/tANS Implementation

#### What FSE actually does

FSE is a table-driven entropy coder. The core idea:

**Encoding**: You have a state (an integer). For each symbol you encode, you look up (state, symbol) in a table to get a new state and some bits to output. The table is constructed so that symbols with higher probability cause fewer bits to be output.

**Decoding**: You have a state. You look it up in a table to get the symbol, the number of bits to read, and the next state base. You read bits, compute the new state, and continue.

#### Table construction (the hard part)

Given a normalized distribution (symbol frequencies that sum to a power of 2, typically 2^accuracy_log where accuracy_log is 5-9):

1. **Spread symbols across the state table** proportional to their frequency. zstd uses a specific deterministic spread function (not random, not optimal, but good and fast).
2. **Build encoding tables**: For each symbol, compute the range of states that correspond to it, and for each state, compute the output bits and next state.
3. **Build decoding tables**: Inverse of encoding — given a state, what symbol does it decode to, and how to compute the next state.

#### Implementation complexity

Based on examining reference implementations and simplified versions:

**Minimal FSE decoder** (decompress only): ~200-300 lines of Go
- Table reconstruction from normalized counts: ~80 lines
- Bit reader: ~50 lines
- Decode loop: ~40 lines
- Table building: ~80 lines

**Minimal FSE encoder** (compress only): ~300-400 lines of Go
- Normalization (convert raw counts to power-of-2 sum): ~100 lines (this is the trickiest part)
- Table construction: ~100 lines
- Bit writer: ~50 lines
- Encode loop: ~50 lines
- Table serialization: ~50 lines

**Both encoder and decoder**: ~500-700 lines of Go for a clean implementation.

**The tricky parts:**
- **Count normalization**: Converting arbitrary symbol frequencies to frequencies that sum to exactly 2^N, while preserving the relative proportions and ensuring every present symbol gets at least frequency 1. Yann Collet's normalization algorithm has subtle edge cases.
- **Bit-level I/O**: FSE reads/writes variable numbers of bits, often straddling byte boundaries. Getting the bit buffer management right (especially flushing) is fiddly.
- **State table spread**: The specific spread algorithm matters for interoperability but any valid spread works for a custom format.

**Comparison**: A Huffman coder is ~200-400 lines total. FSE is roughly 1.5-2x the complexity of Huffman.

#### Existing minimal/educational implementations worth studying:

- **Yann Collet's original FSE repo** (`github.com/Cyan4973/FiniteStateEntropy`) — C reference, ~1500 lines total for the library, but includes multiple variants
- **klauspost/compress** (`github.com/klauspost/compress/fse`) — Go implementation used in production, ~800 lines for FSE proper
- **Jarek Duda's original ANS paper** — the mathematical foundation, explains why ANS achieves Shannon entropy
- **Charles Bloom's blog posts** on ANS/tANS — excellent practical explanations of the algorithm
- **Fabian Giesen's blog** ("Interleaved entropy coders", "A whirlwind tour of ANS") — the clearest explanation of how tANS works mechanically

### 5. Minimum Subset to Match zstd Ratios on Text Data

For natural language and structured text (JSON/EDN), here's the minimum you'd need:

#### Essential components:

1. **LZ77 with large window** (minimum 1MB, ideally 4-8MB for large files)
   - Hash-chain or hash-table match finder (not binary tree — those are for high compression levels)
   - Lazy matching (check if the next position has a better match before committing)
   - Minimum match length of 3 (standard for text)

2. **Repeat offset tracking** (3 recent offsets)
   - Critical for structured text where field spacing repeats
   - Cheap to implement: just a 3-element ring buffer
   - Encode as special offset values 1, 2, 3

3. **Huffman coding for literals**
   - You don't need FSE for literals — zstd itself uses Huffman here
   - Single Huffman table per block is fine
   - On text, literals are the bulk of the data; Huffman is good enough

4. **FSE for sequence streams** (literal-lengths, match-lengths, offsets)
   - This is where FSE matters most — these distributions are highly skewed
   - You could substitute Huffman here and lose ~2-4% compression
   - If you want to truly match zstd ratios, FSE is needed

5. **Baseline + extra bits encoding for sequences**
   - The predefined tables mapping value ranges to (code, baseline, extra-bits) triples
   - You can use zstd's own tables or design similar ones

#### What you can skip and still get close:

- **Dictionary support**: Not needed unless compressing many small documents
- **Multiple block types**: Just use "compressed" blocks
- **Predefined distribution tables**: Always transmit explicit tables
- **Huffman-compressed FSE table headers**: Use a simpler table serialization
- **Content checksums**: Skip for a custom format (add at application layer if needed)
- **Multi-frame support**: Single frame per compression unit
- **Long-distance matching**: Only helps on very large files (>16MB); skip for typical use

#### Rough line count estimate for a minimal "zstd-like" compressor in Go:

| Component | Lines (approx) |
|-----------|----------------|
| LZ77 match finder (hash chain) | 300-400 |
| Repeat offset logic | 50-80 |
| Huffman coder (literals) | 200-300 |
| FSE coder (sequences) | 500-700 |
| Sequence encoding (baseline+extra bits) | 150-200 |
| Bit-level I/O | 100-150 |
| Block framing (minimal) | 100-150 |
| **Total** | **~1400-2000** |

This would get you within 1-3% of zstd's compression ratio on text data at comparable speed. The main thing you'd sacrifice vs full zstd is the optimal parser (which only matters at compression levels 16+, and costs significant CPU).

#### Alternative: FSE everywhere (skip Huffman)

An interesting simplification: use FSE for both literals and sequences. This eliminates Huffman entirely and reduces the number of coding algorithms to implement from 2 to 1. The cost is ~0.5% worse compression on literals (FSE and Huffman are very close on byte-alphabet data) but simpler code. zstd uses Huffman for literals because it's slightly faster to decode, not because it compresses better.

#### The "80% of zstd with 20% of the complexity" approach

If you just want "much better than DEFLATE" without matching zstd exactly:

1. LZ77 with 1MB+ window (~400 lines)
2. Repeat offsets (~60 lines)
3. Huffman for everything (literals + sequences) (~300 lines)
4. Baseline+extra bits for sequences (~150 lines)
5. Minimal framing (~100 lines)

**Total: ~1000 lines.** This gets you the large-window advantage and repeat offsets (the two biggest wins), uses Huffman everywhere (simpler than FSE, gives up ~2-4%), and skips all the format complexity. On text data, this would compress 10-25% better than DEFLATE, vs zstd's 15-30% better than DEFLATE.

### Key References

- **RFC 8878** — the zstd format specification (authoritative but dense)
- **Yann Collet, "Finite State Entropy"** — original FSE description and C implementation
- **Jarek Duda, "Asymmetric numeral systems"** (2009) — the foundational ANS paper
- **Fabian Giesen's blog** — practical tANS explainers, particularly "Interleaved entropy coders"
- **Charles Bloom's blog** ("cbloom rants") — extensive ANS analysis and implementation notes
- **klauspost/compress** — production Go implementation of zstd, FSE, Huffman; good code to study
- **facebook/zstd** — the reference C implementation by Yann Collet

---

## Part 2: Content-Addressable and Order-Preserving Compression

### 1. Order-Preserving Key Compression

#### Foundational Work: Antoshenkov et al. (1996)

The seminal paper is **"Order Preserving String Compression"** by Antoshenkov, Lomet, and Murray (ICDE 1996, IEEE). It introduced a parsing/tokenization technique for variable-length keys that produces compressed output preserving lexicographic order. The core idea: partition the string space into ranges, encode the common prefix of each range. This was implemented in DEC's Rdb relational database. The algorithm (ALM — Antoshenkov-Lomet-Murray) guarantees order preservation of encoded results.

- Paper: [IEEE Xplore](https://ieeexplore.ieee.org/document/492216/)
- Semantic Scholar: [PDF](https://www.semanticscholar.org/paper/Order-Preserving-Key-Compression-Antoshenkov-Lomet/a8df13fd41e53db57d34f453d352e6068d604e42)

#### HOPE: High-speed Order-Preserving Encoder (Zhang et al., SIGMOD 2020)

The most important modern work is **HOPE** from CMU (Zhang, Liu, Andersen, Kaminsky, Keeton, Pavlo). HOPE is a fast dictionary-based compressor that encodes arbitrary keys while preserving their order. It identifies common key patterns at fine granularity and exploits entropy for compression.

Key results:
- **Up to 40% lower query latency** and **up to 30% smaller memory** for in-memory search trees
- Evaluated on 5 data structures: SuRF, ART, HOT, B+tree, Prefix B+tree
- Implements **6 representative compression schemes** making different tradeoffs between compression rate and encoding speed
- Uses a theoretical model for reasoning about order-preserving dictionary designs

This is directly relevant to L85 encoding — HOPE shows that dictionary-based schemes can compress keys while preserving the exact sort order range scans depend on.

- Paper: [CMU PDF](https://db.cs.cmu.edu/papers/2020/zhang-sigmod2020.pdf)
- ArXiv: [2003.02391](https://arxiv.org/abs/2003.02391)

#### Hu-Tucker Optimal Alphabetic Coding

The **Hu-Tucker algorithm** constructs optimal alphabetic binary codes where codeword ordering matches the natural order of the source symbols. Unlike Huffman coding (which can reorder), Hu-Tucker guarantees the alphabetic/lexicographic constraint is preserved while minimizing total code length. This is the theoretical foundation for order-preserving variable-length codes.

- MIT lecture notes: [Hu-Tucker](https://math.mit.edu/~djk/18.310/Lecture-Notes/PeterShor-hu-tucker.html)

### 2. Searchable / Homomorphic Compression

#### HOCO / HocoPG (Guan et al., SIGMOD 2023 / VLDB 2024)

**Homomorphic Compression (HOCO)** is the most significant work here. It allows performing text operations directly on compressed data without decompression. Three compression schemes were implemented with homomorphism support:

- **9.18x higher throughput** for random access and modification operations vs. state-of-the-art
- **7.16x lower latency** for text analytics vs. uncompressed processing
- Supports string matching, replacement, substring extraction, and full-text search on compressed form
- **HocoPG** integrates this into PostgreSQL: data is automatically compressed on INSERT, all subsequent queries operate on compressed form

This is the strongest example of "operate on compressed data" in the literature.

- HOCO paper: [ACM DL](https://dl.acm.org/doi/10.1145/3626765)
- HocoPG: [VLDB PDF](https://www.vldb.org/pvldb/vol17/p4477-guan.pdf)

#### FSST: Fast Static Symbol Table (Boncz et al., VLDB 2020)

**FSST** from CWI Amsterdam is a lightweight string compression scheme used in column stores. Its key property: **equality comparisons can be performed directly on compressed values** as long as both operands are compressed with the same symbol table. This means queries with equality-selection predicates work on compressed strings without decompression.

Properties:
- Maps 1-8 byte "symbols" to single-byte "codes"
- Random access to individual compressed strings (not block-based)
- Decompression speed comparable to or better than LZ4, with significantly better compression
- Implemented in DuckDB and other column stores

FSST supports equality but NOT order comparison on compressed form — the compression does not preserve sort order.

- Paper: [VLDB PDF](https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf)
- Implementation: [GitHub cwida/fsst](https://github.com/cwida/fsst), [Go port](https://github.com/axiomhq/fsst)

#### CompressDB (Zhang et al., SIGMOD 2022)

**CompressDB** uses context-free grammar to compress data and supports both query and manipulation without decompression. It integrates at the filesystem level so databases (SQLite, MySQL, LevelDB, MongoDB, ClickHouse, Neo4j) can use it transparently.

Results: 40% throughput improvement, 44% latency reduction, 1.75x compression ratio.

- Paper: [ACM DL](https://dl.acm.org/doi/10.1145/3514221.3526130)

#### Order-Preserving Run-Length Encoding (US Patent 5,619,199)

IBM patented a scheme specifically for **comparing RLE-compressed keys without decompression**. When a mismatch occurs during comparison, the algorithm extracts codewords (compressed character + escape character + count) and compares them directly. This enables B-tree index traversal on compressed keys.

- Patent: [US5619199](https://patents.google.com/patent/US5619199)

### 3. Order-Preserving Encodings (Not Encryption)

This category is highly relevant to janus-datalog's L85 encoding.

#### Memcomparable Format (TiDB / RisingWave)

TiDB uses a **Memcomparable** encoding where the comparison result of two objects before encoding is consistent with byte-array comparison after encoding. All row data is arranged in TiKV by RowID order, enabling efficient range scans on encoded keys.

Key techniques:
- **Integers**: Big-endian, sign-bit flipping (signed integers XOR with 0x80 in MSB)
- **Strings**: Chunked encoding with padding and continuation bytes
- **Floats**: IEEE 754 with conditional bit flipping

- TiDB docs: [TiDB Computing](https://docs.pingcap.com/tidb/stable/tidb-computing/)
- Rust implementation: [risingwavelabs/memcomparable](https://github.com/risingwavelabs/memcomparable)

#### FoundationDB Tuple Layer

FoundationDB's tuple layer is the gold standard for order-preserving multi-type encoding:

- **Type code prefix** (single byte) identifies each element
- **Integers**: Variable-length encoding with 17 typecodes (0x0c-0x1c). Negative numbers use big-endian one's complement. Zero is 0x14. Supports arbitrary precision via typecodes 0x0b/0x1d.
- **Strings**: Null-terminated with null bytes escaped as 0x00 0xFF (order-preserving escape)
- **Floats**: IEEE big-endian with conditional bit flipping: "if sign bit set, flip all bits; otherwise flip only sign bit"
- **Nested tuples**: Recursive encoding with 0x05 typecode
- **Key property**: Common tuple prefix serializes as common byte prefix

This is architecturally very similar to what L85 does for janus-datalog, but more general.

- Design doc: [GitHub](https://github.com/apple/foundationdb/blob/main/design/tuple.md)

#### CockroachDB Key Encoding

CockroachDB encodes SQL primary keys into KV keys such that `enc(x) <= enc(y) iff x <= y` for ascending columns (reversed for descending). Uses a prefix-free encoding where the first byte indicates field type. STRING and BYTES share an encoding. Collated strings appear twice: once as sort key, once as actual value.

- Tech notes: [encoding.md](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md)
- Encoding code: [encoding.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go)

#### Other Notable Implementations

- **[bytekey](https://github.com/danburkert/bytekey)** (Rust): Lexicographic sort-order preserving binary encoding for LevelDB-style stores
- **[orderly](https://github.com/ndimiduk/orderly)** (Java): Schema and type system for creating sortable byte arrays (HBase)
- **[StatelyDB](https://stately.cloud/blog/encoding-sortable-binary-database-keys)**: Uses a "sort byte" prefix encoding both length and sign for integers

#### Sortable Base64/Base85

Standard Base64 and Base85 do NOT preserve sort order because the alphabet ordering doesn't match byte value ordering. Solutions require custom alphabets where character ordering matches the underlying byte ordering. L85 already does this correctly — the L85 alphabet is specifically chosen to be in ASCII sort order, so lexicographic comparison of L85-encoded strings matches the byte comparison of the underlying binary data.

### 4. How Real Database Systems Handle Key/Value Compression

#### Approach A: Compress values, keep keys sortable (most common)

Most LSM-tree and B-tree databases do NOT compress their keys with general-purpose compression. Instead they:

1. **Use order-preserving encodings** for keys (memcomparable, tuple layer, etc.)
2. **Apply prefix compression** within sorted blocks
3. **Compress values** with general-purpose algorithms (LZ4, zstd, Snappy)

#### LevelDB / RocksDB / Pebble: Block-level prefix compression

Keys are delta-encoded within blocks:
- Each entry stores `(shared_bytes, unshared_bytes, value_length, key_delta, value)`
- **Restart points** every N keys (default 16) store the full key for binary search
- This is NOT general compression — it's prefix deduplication of sorted keys
- Range scans work because the block is decompressed in memory; within a block, binary search uses restart points

RocksDB also supports **prefix bloom filters** for efficient prefix-based seeks, and **zstd dictionary compression** across blocks within an SSTable for better value compression.

Pebble (CockroachDB's Go storage engine) uses the same block format. There is an [open issue for dictionary compression](https://github.com/cockroachdb/pebble/issues/3453) to improve cross-block compression.

#### WiredTiger (MongoDB): Prefix compression for indexes

- Default for all indexes: adjacent key prefix compression
- First key per page stored in full, subsequent keys store only the suffix differing from predecessor
- Also uses suffix truncation: when splitting nodes, shorter separator keys replace full keys
- Block-level Snappy compression for collections

#### InnoDB (MySQL): Page-level compression

- B-tree pages compressed as whole units
- Uncompressed "modification log" within compressed pages avoids recompression on small updates
- Key prefix compression within pages

#### ClickHouse: Column-level specialized encodings

ClickHouse uses a two-layer approach:
1. **Encodings** (Delta, DoubleDelta, Gorilla, T64) transform data by type to reduce entropy
2. **Compression** (LZ4, ZSTD) compresses the encoded output

These encodings do NOT preserve sort order — they're designed for columnar analytics where you decompress before comparing. Sort order is maintained by the MergeTree's primary key ordering, not by compression.

#### DuckDB: Lightweight compression

Seven algorithms (Constant, RLE, BitPacking, FOR, Dictionary, FSST, Chimp/Patas), selected per-column-segment. Dramatic compression ratios (e.g., 1.73GB to 0.21GB for On Time dataset). Operates in conjunction with late materialization and vectorized execution.

#### Datomic: Fressian + zip

Datomic segments (arrays of ~1000-20000 datoms) are serialized with **Fressian** (a byte-code-driven extensible binary format), then compressed with zip, producing ~50KB segments. Sort order is maintained at the segment/B-tree level (segment boundaries are sorted keys), but within a segment, data must be decompressed before querying. The B-tree has 1000+ branching factor and only ~3 levels deep.

#### Apache Parquet: Dictionary encoding + min/max statistics

Parquet uses dictionary encoding and RLE within column chunks. Range scans work through **predicate pushdown** using per-chunk min/max statistics — the query engine skips chunks whose range doesn't overlap the predicate. Dictionary-level predicate evaluation can provide up to 8x speedup.

### 5. B-Tree Compression Survey (SIGMOD 2024)

**"Revisiting B-tree Compression: An Experimental Study"** evaluated 7 compression techniques:
- Prefix compression (used by WiredTiger, InnoDB)
- Suffix truncation (used by WiredTiger)
- Head+Tail compression (recommended — best search/insert performance with decent compression)
- Partial-key approaches (SAP HANA's CPB-tree)
- Front coding (MyISAM)

Key finding: **Head+Tail Compression** achieves faster performance than uncompressed B-trees while maintaining decent compression ratio, because shorter keys mean more keys per node means fewer cache misses.

- Paper: [Purdue PDF](https://www.cs.purdue.edu/homes/csjgwang/pubs/SIGMOD24_BtreeCompression.pdf)

### 6. Dictionary-Based Order-Preserving String Compression (Binnig et al., SIGMOD 2009)

Designed for SAP HANA-style column stores: replaces variable-length strings with fixed-length integer codes from an order-preserving dictionary. The dictionary maps string values to integer codes such that the code ordering matches the string ordering. Enables range queries on codes without decompression. Uses front coding internally (lexicographic ordering + prefix deduplication).

- Paper: [CMU Course PDF](https://15721.courses.cs.cmu.edu/spring2016/papers/p283-binnig.pdf)

### 7. The Fundamental Tension

LZ77 replaces repeated substrings with (offset, length) back-references. The encoded form of "apple" depends on whether "apple" appeared earlier in the stream and where. Two different strings can compress to forms where the lexicographic relationship is reversed. There is no way to fix this without giving up the back-referencing that makes LZ77 effective.

Order-preserving schemes are constrained to **per-symbol or per-pattern mappings** where code ordering matches input ordering. This rules out cross-reference between positions — which is exactly what gives LZ77 its power.

**This tension is fundamental, not an implementation gap.** No future algorithm will achieve both LZ77-level ratios and sort-order preservation on general data.

### 8. Summary and Relevance to L85

L85 is already an **order-preserving encoding** (category 3 above). It maps 20-byte binary values to 25-character printable strings while preserving lexicographic sort order, using a carefully chosen 85-character alphabet in ASCII sort order with big-endian encoding.

The research suggests several potential directions:

**If the goal is to make keys shorter while preserving sort order:**
- HOPE's dictionary-based approach could compress common key patterns while maintaining order. This is the most directly applicable academic work.
- Hu-Tucker coding provides the theoretical optimum for order-preserving variable-length codes.

**If the goal is to compress values while still supporting range scans:**
- The standard approach (LevelDB/RocksDB/Pebble) is: keep keys in order-preserving encoding, use prefix deduplication within sorted blocks, compress values with LZ4/zstd. This is what BadgerDB already does.
- Dictionary compression across blocks (as proposed for Pebble) would improve value compression for sorted index data.

**If the goal is to support operations on compressed data:**
- FSST for equality comparison on compressed strings
- HOCO/HocoPG for text operations on compressed data
- Order-preserving dictionary encoding (Binnig et al.) for range queries on compressed dictionary codes

**The gap in the literature**: There is no system that does true general-purpose compression of keys while preserving arbitrary range scan capability on the compressed form. Every production database either (a) uses order-preserving *encoding* (not compression) for keys, or (b) compresses at the block/page level and decompresses for comparison. HOPE is the closest to bridging this gap, but it's dictionary-based encoding, not general compression.

### All References

- [HOPE paper — CMU SIGMOD 2020](https://db.cs.cmu.edu/papers/2020/zhang-sigmod2020.pdf)
- [HOPE on ArXiv](https://arxiv.org/abs/2003.02391)
- [Antoshenkov et al. — IEEE ICDE 1996](https://ieeexplore.ieee.org/document/492216/)
- [HOCO — ACM SIGMOD 2023](https://dl.acm.org/doi/10.1145/3626765)
- [HocoPG — VLDB 2024](https://www.vldb.org/pvldb/vol17/p4477-guan.pdf)
- [FSST — VLDB 2020](https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf)
- [FSST GitHub](https://github.com/cwida/fsst)
- [FSST Go port](https://github.com/axiomhq/fsst)
- [CompressDB — SIGMOD 2022](https://dl.acm.org/doi/10.1145/3514221.3526130)
- [Order-Preserving RLE Patent US5619199](https://patents.google.com/patent/US5619199)
- [FoundationDB Tuple Design](https://github.com/apple/foundationdb/blob/main/design/tuple.md)
- [CockroachDB Encoding](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md)
- [TiDB Computing / Memcomparable](https://docs.pingcap.com/tidb/stable/tidb-computing/)
- [risingwavelabs/memcomparable (Rust)](https://github.com/risingwavelabs/memcomparable)
- [Memcmp-friendly encoding blog](https://yizhang82.dev/sorting-structured-data-1)
- [StatelyDB key encoding](https://stately.cloud/blog/encoding-sortable-binary-database-keys)
- [Lexicographic sort order blog](https://cornerwings.github.io/2019/10/lexical-sorting/)
- [bytekey (Rust)](https://github.com/danburkert/bytekey)
- [orderly (Java)](https://github.com/ndimiduk/orderly)
- [B-tree Compression Survey — SIGMOD 2024](https://www.cs.purdue.edu/homes/csjgwang/pubs/SIGMOD24_BtreeCompression.pdf)
- [Binnig et al. Dictionary Compression — SIGMOD 2009](https://dl.acm.org/doi/abs/10.1145/1559845.1559877)
- [SuRF — SIGMOD 2018](https://www.pdl.cmu.edu/PDL-FTP/Storage/surf_sigmod18.pdf)
- [Hu-Tucker Algorithm](https://math.mit.edu/~djk/18.310/Lecture-Notes/PeterShor-hu-tucker.html)
- [DuckDB Lightweight Compression](https://duckdb.org/2022/10/28/lightweight-compression)
- [RocksDB Prefix Seek](https://github.com/facebook/rocksdb/wiki/Prefix-Seek)
- [Pebble dictionary compression issue](https://github.com/cockroachdb/pebble/issues/3453)
- [WiredTiger compression](http://source.wiredtiger.com/2.3.0/file_formats.html)
- [Datomic Internals](https://tonsky.me/blog/unofficial-guide-to-datomic-internals/)
- [ClickHouse compression docs](https://clickhouse.com/docs/data-compression/compression-in-clickhouse)
- [LevelDB prefix compression](https://medium.com/@xuezaigds/leveldb-explained-prefix-compression-and-restart-points-in-blockbuilder-5ebeb51c2b0d)
- [Boldyreva et al. OPE — Crypto 2009](https://link.springer.com/chapter/10.1007/978-3-642-01001-9_13)
- [Andy Pavlo CMU 15-721 Compression Lecture](https://15721.courses.cs.cmu.edu/spring2023/slides/05-compression.pdf)
- [RFC 8878 — zstd format specification](https://www.rfc-editor.org/rfc/rfc8878)
- [Yann Collet, FiniteStateEntropy (GitHub)](https://github.com/Cyan4973/FiniteStateEntropy)
- [Jarek Duda, "Asymmetric numeral systems" (2009)](https://arxiv.org/abs/0902.0271)
- [klauspost/compress (Go)](https://github.com/klauspost/compress)
- [facebook/zstd (C reference)](https://github.com/facebook/zstd)