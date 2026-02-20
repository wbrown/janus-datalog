# Database Export/Import

This document describes the database export and import functionality for backing up, migrating, and sharing Janus Datalog databases.

## Overview

Janus Datalog supports exporting the entire database to a human-readable EDN (Extensible Data Notation) format and importing it back. This enables:

- **Backup and restore** - Create portable backups of your database
- **Migration** - Move data between systems or database versions
- **Debugging** - Inspect database contents in a readable format
- **Data sharing** - Share datasets in a standard format

## EDN Format

Each line in the export file is a complete EDN vector representing a single datom:

```edn
[#identity "L85hash25chars" :attribute value tx-id]
```

### Example Output

```edn
[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/name "Alice" 1]
[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/age 30 1]
[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/email "alice@example.com" 1]
[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/created #inst "2025-01-15T10:30:00Z" 1]
[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/avatar #bytes "L85encodeddata" 1]
[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/friend #identity "1$&2Ku;N<k)8Q"7t1CwE5l".-"" 2]
```

### Value Type Mappings

| Go Type | EDN Format | Example |
|---------|------------|---------|
| `string` | quoted string | `"Alice"` |
| `int64` | integer | `12345` |
| `float64` | float | `3.14` |
| `bool` | boolean | `true` / `false` |
| `time.Time` | instant tag | `#inst "2025-01-15T10:30:00Z"` |
| `[]byte` | bytes tag (L85) | `#bytes "L85encoded"` |
| `Identity` | identity tag | `#identity "L85hash"` |
| `Keyword` | keyword | `:namespace/name` |
| `Symbol` | symbol | `my-symbol` |

### Reader Tags

- `#identity "L85string"` - Entity identity (always 25-character L85 encoding of SHA1 hash)
- `#inst "RFC3339"` - Timestamp in RFC3339 format, always UTC
- `#bytes "L85encoded"` - Byte array in L85 encoding (self-describing length)

## CLI Usage

### Export

Export a database to an EDN file:

```bash
datalog -db mydata.db -export backup.edn
```

### Import

Import an EDN file into a database (creates the database if it doesn't exist):

```bash
datalog -db newdata.db -import backup.edn
```

### Round-Trip Verification

```bash
# Export original
datalog -db original.db -export data.edn

# Import to new database
datalog -db copy.db -import data.edn

# Export copy and compare (should be identical)
datalog -db copy.db -export data2.edn
diff data.edn data2.edn
```

## Go API Usage

### Export

```go
import (
    "os"
    "github.com/wbrown/janus-datalog/datalog/db"
)

// Open database
d, err := db.Open("mydata.db")
if err != nil {
    log.Fatal(err)
}
defer d.Close()

// Export to file
f, err := os.Create("backup.edn")
if err != nil {
    log.Fatal(err)
}
defer f.Close()

if err := d.Export(f); err != nil {
    log.Fatal(err)
}
```

### Import

```go
// Open/create database
d, err := db.Open("newdata.db")
if err != nil {
    log.Fatal(err)
}
defer d.Close()

// Import from file
f, err := os.Open("backup.edn")
if err != nil {
    log.Fatal(err)
}
defer f.Close()

if err := d.Import(f); err != nil {
    log.Fatal(err)
}
```

### Export to Buffer (for testing or in-memory use)

```go
var buf bytes.Buffer
if err := d.Export(&buf); err != nil {
    log.Fatal(err)
}

// Use buf.String() or buf.Bytes()
```

## Format Details

### Entity Identities

Entity identities are always exported as L85-encoded SHA1 hashes (20 bytes = 25 characters). This ensures:

- **Consistency** - Same entity always produces same output
- **Portability** - No dependency on internal database IDs
- **Sortability** - L85 encoding preserves lexicographic order

### Transaction IDs

Transaction IDs are preserved during export/import, allowing exact restoration of database state including the transaction history structure.

### Comments and Blank Lines

During import:
- Lines starting with `;` are treated as comments and ignored
- Blank lines are ignored
- This allows annotating export files for documentation

Example with comments:
```edn
; Person records
[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/name "Alice" 1]
[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/age 30 1]

; Friendship relations
[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/friend #identity "1$&2Ku;N<k)8Q"7t1CwE5l".-"" 2]
```

## Performance

### Export

- Streams directly from EAVT index
- Uses buffered I/O for efficiency
- Memory usage is constant regardless of database size

### Import

- Batched transactions (5000 datoms per batch)
- Uses 1MB line buffer for long values
- Efficient for large imports

## Error Handling

### Import Errors

Import errors include the line number for easy debugging:

```
line 42: invalid EDN: unexpected character
line 100: invalid entity: invalid L85 in #identity: expected 25 characters, got 20
```

### Validation

The import process validates:
- EDN syntax correctness
- Vector format (exactly 4 elements: entity, attribute, value, tx)
- Entity identity format (valid L85, 25 characters)
- Attribute format (must be a keyword)
- Transaction ID format (must be an integer)

## Limitations

- **No incremental export** - Always exports the entire database
- **No schema export** - Schema definitions are not included (schema is optional and additive)
- **No compression** - Output is plain text (use external compression if needed)

## Related Documentation

- [L85 Encoding](../../CLAUDE.md#l85-encoding-details) - Details on the L85 encoding used for identities and bytes
- [Storage Design](../../CLAUDE.md#storage-design) - Overview of the EAVT storage model
