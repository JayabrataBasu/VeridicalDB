# VeridicalDB

> *Veridical: truthful, corresponding to facts*

A modern, embeddable database engine built from scratch in Go.

VeridicalDB supports both row-based (heap) and columnar storage, MVCC transactions, SQL queries, and is designed for both embedded and client-server use cases.

## Current Status: Stage 0 - Foundation ✅

The project is being built incrementally. See [Roadmap.md](Roadmap.md) for the full development plan.

## Quick Start

### Prerequisites

- Go 1.21 or later
- Make (optional, but recommended)

### Build

```bash
# Clone the repository
git clone https://github.com/JayabrataBasu/VeridicalDB.git
cd VeridicalDB

# Download dependencies
go mod download

# Build
make build
# Or without make:
go build -o build/veridicaldb ./cmd/veridicaldb
```

### Initialize a Database

```bash
# Create a new database directory
./build/veridicaldb init ./data

# This creates:
#   ./data/           - Main data directory
#   ./data/wal/       - Write-ahead log
#   ./data/tables/    - Table storage
#   ./data/indexes/   - Index storage
#   ./veridicaldb.yaml - Configuration file
```

### Run

```bash
# Start the interactive shell
./build/veridicaldb --config veridicaldb.yaml
```

You'll see:

```
 __      __        _     _ _           _ ____  ____  
 \ \    / /       (_)   | (_)         | |  _ \|  _ \ 
  \ \  / /__ _ __  _  __| |_  ___ __ _| | | | | |_) |
   \ \/ / _ \ '__|| |/ _' | |/ __/ _' | | | | |  _ < 
    \  /  __/ |   | | (_| | | (_| (_| | | |_| | |_) |
     \/ \___|_|   |_|\__,_|_|\___\__,_|_|____/|____/ 

    Version 0.1.0 - Stage 0: Foundation
    Type HELP; or \? for available commands

veridicaldb> 
```

### Available Commands

```
veridicaldb> HELP;        -- Show help
veridicaldb> \status      -- Show server status
veridicaldb> \config      -- Show configuration
veridicaldb> \dt          -- List tables (coming in Stage 2)
veridicaldb> EXIT;        -- Exit the shell
```

## Configuration

VeridicalDB can be configured via:

1. **Config file** (YAML, JSON, or TOML):
   ```yaml
   server:
     host: localhost
     port: 5433
   storage:
     data_dir: ./data
     page_size: 8192
     buffer_pool_mb: 128
   log:
     level: info
     format: text
   ```

2. **Environment variables** (prefix: `VERIDICAL_`):
   ```bash
   export VERIDICAL_SERVER_PORT=5433
   export VERIDICAL_STORAGE_DATA_DIR=/var/lib/veridicaldb
   export VERIDICAL_LOG_LEVEL=debug
   ```

## Development

```bash
# Run tests
make test

# Run tests with coverage
make test-coverage

# Format code
make fmt

# Build for all platforms
make build-all

# Full development cycle
make dev
```

## Project Structure

```
VeridicalDB/
├── cmd/
│   └── veridicaldb/      # Main binary entry point
├── internal/
│   ├── cli/              # REPL and command handling
│   ├── config/           # Configuration management
│   ├── logger/           # Structured logging
│   ├── storage/          # Storage engine (Stage 1+)
│   ├── catalog/          # Metadata management (Stage 2+)
│   ├── sql/              # SQL parser & executor (Stage 3+)
│   └── txn/              # Transaction manager (Stage 4+)
├── pkg/                  # Public APIs (future)
├── Makefile
├── Roadmap.md
└── README.md
```

## Roadmap

| Stage | Description | Status |
|-------|-------------|--------|
| 0 | Foundation (CLI, Config, Logging) | ✅ Complete |
| 1 | Heap Storage Engine | ✅ Complete |
| 2 | Catalog & Schema | ✅ Complete |
| 3 | SQL Parser & Executor | ✅ Complete |
| 4 | MVCC Transactions | 🔜 Next |
| 5 | Concurrency & Locking | ⏳ Planned |
| 6 | B+ Tree Indexes | ⏳ Planned |
| 7 | WAL & Recovery | ⏳ Planned |
| 8 | Columnar Storage | ⏳ Planned |
| 9 | Sharding | ⏳ Planned |
| 10 | Polish & Observability | ⏳ Planned |

See [Roadmap.md](Roadmap.md) for detailed specifications.

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please read the roadmap first to understand the project direction.