# HyperVault Database

A production-ready SQL database implementation in Rust featuring ACID transactions, multi-threaded concurrency control, write-ahead logging (WAL) for crash recovery, and advanced SQL operations.

**Version:** 1.2.0

## Core Features

### ACID Transactions
Full transaction support with `BEGIN TRANSACTION`, `COMMIT`, and `ROLLBACK`. Transactions maintain read/write sets and ensure atomicity through WAL entries. Each transaction is assigned a unique ID and tracked through its lifecycle.

### Write-Ahead Logging (WAL)
All write operations are logged to a WAL file before being applied to the database. On crash recovery, the system replays committed transactions from the WAL, ensuring data durability. Checkpointing periodically flushes WAL entries to the main database file.

### Multi-threaded Concurrency Control
Table-level locking with shared (read) and exclusive (write) locks managed via `parking_lot::RwLock`. The lock manager tracks lock holders and waiters, preventing deadlocks through transaction ordering. Multiple readers can access tables concurrently, while writers require exclusive access.

### Advanced SQL Operations
- **JOINs**: Inner, left, and right joins with configurable join conditions
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX with optional column filtering
- **GROUP BY**: Grouping with aggregate functions
- **ORDER BY**: Sorting with ASC/DESC on multiple columns
- **LIMIT/OFFSET**: Pagination support
- **WHERE clauses**: Complex filtering with operators (=, >, <, >=, <=, !=, <>)

### Query Optimization
Query planner analyzes query complexity (Simple, Moderate, Complex) and generates execution plans with cost estimation. Statistics tracking monitors query performance and table metadata (row counts, column selectivity) to inform optimization decisions.

### Encryption
Optional AES-GCM encryption for database files. Passwords are hashed using Argon2 or PBKDF2, with derived keys cached for performance. Encryption keys are managed separately from passwords, allowing password changes without full re-encryption.

### B+ Tree Indexing
Index structures for efficient lookups and range queries. Indexes are maintained per table-column combination and integrated into query planning for optimal access paths.

### Storage Engine
File-based persistence using `bincode` serialization. Tables are stored as in-memory hash maps with metadata tracking (version, timestamps, operation counts). The storage engine coordinates with transaction and WAL managers for consistent state management.

## Quick Start

```bash
# Build the project
cargo build --release

# Run the database
cargo run

# Or run the release binary
./target/release/hyper_vault
```

On first run, you'll be prompted to set an encryption password (optional). The database creates a `database.db` file and initializes a sample `users` table.

## Usage Examples

```sql
-- Basic queries
SELECT * FROM users;
SELECT id, name FROM users WHERE age > '25';
SELECT COUNT(*) FROM users;

-- Advanced queries
SELECT * FROM users ORDER BY age DESC LIMIT 10;
SELECT * FROM table1 JOIN table2 ON table1.id = table2.id;
SELECT department, COUNT(*) FROM employees GROUP BY department;

-- Data manipulation
INSERT INTO users (id, name, email, age) VALUES ('5', 'John Doe', 'john@example.com', '32');
UPDATE users SET age = '26' WHERE id = '1';
DELETE FROM users WHERE age > '35';

-- Transactions
BEGIN TRANSACTION;
INSERT INTO users (id, name) VALUES ('6', 'Alice');
UPDATE users SET age = '30' WHERE id = '6';
COMMIT TRANSACTION;
```

## CLI Commands

- `help` - Show available commands
- `show tables` - List all tables with schema information
- `show all` - Display all data from all tables
- `show stats` - Show database and query statistics
- `set password <pwd>` - Set encryption password
- `reset password` - Change encryption password
- `clear password` - Remove encryption
- `exit` - Exit the database

## Architecture Overview

The database follows a modular architecture:

- **Parser/Lexer**: SQL parsing using `nom` parser combinators, generating AST nodes
- **Query Planner**: Analyzes queries, estimates costs, and generates execution plans
- **Executor**: Executes query plans, manages transactions, and coordinates with storage
- **Storage Engine**: Manages table schemas, row storage, and file I/O
- **Transaction Manager**: Tracks active transactions, read/write sets, and WAL entries
- **WAL Manager**: Appends log entries, handles recovery, and performs checkpoints
- **Lock Manager**: Coordinates concurrent access with table-level locks
- **Encryption**: Handles key derivation, encryption/decryption of database files

## Implementation Notes

- **Serialization**: Database state is serialized using `bincode` for efficient binary storage
- **Locking Strategy**: Table-level granularity with reader-writer locks for optimal concurrency
- **Recovery**: WAL entries are replayed on startup, filtering out aborted transactions
- **Query Planning**: Cost-based optimization considers table statistics and query complexity
- **Index Management**: B+ trees are maintained per column for fast lookups and range scans

## Dependencies

- `bincode` - Binary serialization
- `nom` - Parser combinators for SQL parsing
- `serde` - Serialization framework
- `aes-gcm` - AES-GCM encryption
- `argon2`, `pbkdf2` - Password hashing
- `parking_lot` - High-performance locks
- `uuid` - Transaction ID generation

## License

MIT OR Apache-2.0

