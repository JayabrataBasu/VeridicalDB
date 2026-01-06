# VeridicalDB - Quick Start Guide

Download the right file for your system and follow the steps below.

---

## 📦 Which File Do I Download?

| Your System | Download This |
|-------------|---------------|
| **Linux** (Mint, Ubuntu, Debian, etc.) | `veridicaldb-linux.tar.gz` |
| **Windows** (Intel/AMD PC) | `veridicaldb-windows.zip` |
| **Mac** (M1/M2/M3 Apple Silicon) | `veridicaldb-mac-silicon.tar.gz` |

---

## 🐧 Linux (Mint, Ubuntu, etc.)

### Step 1: Extract
Open Terminal and run:
```bash
tar -xzf veridicaldb-linux.tar.gz
cd veridicaldb
```

### Step 2: Initialize (first time only)
```bash
./veridicaldb init
```
This creates a `data` folder and config file.

### Step 3: Run
```bash
./veridicaldb
```

That's it! You should see the VeridicalDB banner and prompt.

### Troubleshooting
If you get "Permission denied":
```bash
chmod +x veridicaldb
./veridicaldb
```

---

## 🪟 Windows

### Step 1: Extract
1. Right-click `veridicaldb-windows.zip`
2. Click **"Extract All..."**
3. Choose a location (e.g., Desktop)
4. Click **Extract**

### Step 2: Initialize (first time only)
Open PowerShell or Command Prompt:
```powershell
cd C:\Users\YourName\Desktop\veridicaldb
.\veridicaldb.exe init
```
This creates a `data` folder and config file.

### Step 3: Run
**Option A - Command line:**
```powershell
.\veridicaldb.exe
```

**Option B - Double-click:**
After running `init` once, you can double-click `veridicaldb.exe` to start.

### Troubleshooting

**"Windows protected your PC" warning:**
1. Click **"More info"**
2. Click **"Run anyway"**

**"Data directory does not exist" error:**
Run `.\veridicaldb.exe init` first (Step 2 above).

**Window closes immediately:**
Run from PowerShell to see error messages.

**"veridicaldb.exe is not recognized":**
Make sure you're in the correct folder. Use `dir` to list files.

---

## 🍎 Mac (Apple Silicon - M1/M2/M3)

### Step 1: Extract
Open Terminal and run:
```bash
tar -xzf veridicaldb-mac-silicon.tar.gz
cd veridicaldb
```

### Step 2: Initialize (first time only)
```bash
./veridicaldb init
```
This creates a `data` folder and config file.

### Step 3: Run
```bash
./veridicaldb
```

### Troubleshooting

**"cannot be opened because the developer cannot be verified":**
```bash
xattr -d com.apple.quarantine veridicaldb
./veridicaldb
```

**Permission denied:**
```bash
chmod +x veridicaldb
./veridicaldb
```

---

## ✅ Verify Your Download (Optional)

Check that your download wasn't corrupted:

**Linux/Mac:**
```bash
sha256sum veridicaldb-linux.tar.gz
# Compare with the hash in SHA256SUMS file
```

**Windows (PowerShell):**
```powershell
Get-FileHash veridicaldb-windows.zip -Algorithm SHA256
```

---

## 🎮 Basic Usage

Once VeridicalDB is running, try these commands:

```sql
-- Create a table
CREATE TABLE users (id INT, name TEXT);

-- Insert data
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

-- Query data
SELECT * FROM users;

-- Create a view
CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = true;

-- Query the view
SELECT * FROM active_users;

-- Exit
\quit
```

---

## 🔐 TLS/SSL Configuration

VeridicalDB supports TLS encryption for secure client connections using the PostgreSQL wire protocol (pgwire). This protects your data in transit and is recommended for production deployments.

### Generating Self-Signed Certificates (Development)

For development and testing, you can generate self-signed certificates:

```bash
# Create a certificate directory
mkdir -p certs
cd certs

# Generate a private key
openssl genrsa -out server.key 2048

# Generate a self-signed certificate (valid for 365 days)
openssl req -new -x509 -key server.key -out server.crt -days 365 \
  -subj "/CN=localhost/O=VeridicalDB"

# Set proper permissions
chmod 600 server.key
chmod 644 server.crt
```

### Configuring TLS in VeridicalDB

Add the following to your `config.yaml`:

```yaml
pgwire:
  tls:
    enabled: true
    cert_file: "./certs/server.crt"
    key_file: "./certs/server.key"
    # Optional: for client certificate authentication (mTLS)
    # ca_file: "./certs/ca.crt"
    # client_auth: "require_and_verify"
    min_version: "1.2"
```

### Client Connection Examples

**psql with TLS:**
```bash
# Basic TLS (server verification disabled)
psql "host=localhost port=5432 user=myuser dbname=mydb sslmode=require"

# TLS with server certificate verification
psql "host=localhost port=5432 user=myuser dbname=mydb sslmode=verify-full sslrootcert=./certs/ca.crt"

# mTLS (mutual TLS with client certificate)
psql "host=localhost port=5432 user=myuser dbname=mydb sslmode=verify-full \
  sslrootcert=./certs/ca.crt \
  sslcert=./certs/client.crt \
  sslkey=./certs/client.key"
```

**Connection string SSL modes:**
- `disable` - No SSL
- `allow` - Try non-SSL, then SSL
- `prefer` - Try SSL, then non-SSL (default for most clients)
- `require` - Require SSL, skip server verification
- `verify-ca` - Require SSL and verify server cert is signed by trusted CA
- `verify-full` - Require SSL, verify cert and hostname match

### Client Authentication Policies

The `client_auth` setting controls client certificate requirements:

| Value | Description |
|-------|-------------|
| `none` | Don't request client certificate (default) |
| `request` | Request client cert but don't require it |
| `require` | Require client cert but don't verify against CA |
| `verify` | Request and verify client cert if provided |
| `require_and_verify` | Require and verify client cert (full mTLS) |

### Generating Certificates for mTLS (Production)

For production mTLS deployments:

```bash
# 1. Create a CA (Certificate Authority)
openssl genrsa -out ca.key 4096
openssl req -new -x509 -key ca.key -out ca.crt -days 3650 \
  -subj "/CN=VeridicalDB CA/O=YourOrganization"

# 2. Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
  -subj "/CN=your-server-hostname/O=YourOrganization"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt -days 365

# 3. Generate client certificate
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
  -subj "/CN=client-name/O=YourOrganization"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt -days 365
```

### Security Best Practices

1. **Use TLS 1.2 or higher** - Set `min_version: "1.2"` (default)
2. **Protect private keys** - Use `chmod 600` on key files
3. **Rotate certificates** - Plan for certificate renewal before expiry
4. **Use proper hostnames** - Ensure certificates include the correct CN/SAN
5. **Consider mTLS** - For high-security environments, require client certificates

---

## 💾 Backup and Point-in-Time Recovery (PITR)

VeridicalDB provides built-in backup and point-in-time recovery capabilities for disaster recovery and data protection.

### Quick Start

```bash
# Create a base backup
veridicaldb backup basebackup

# List all backups
veridicaldb backup list

# Verify a backup
veridicaldb backup verify /path/to/backup.tar.gz

# Archive current WAL
veridicaldb wal archive

# List archived WAL segments
veridicaldb wal list
```

### Creating Base Backups

A base backup is a complete copy of your database at a specific point in time.

```bash
# Create backup with default settings (compressed, stored in data/backups/)
veridicaldb backup basebackup

# Create backup to a specific location
veridicaldb backup basebackup --output /backups/mydb_backup

# Output:
# Creating base backup...
# Backup completed successfully!
#   ID:        backup_20260106_153000
#   Size:      1048576 bytes
#   Start LSN: 1234
#   End LSN:   5678
#   Duration:  2.5s
```

### Configuring Backups

Add to your `config.yaml`:

```yaml
backup:
  # Directory for storing backups
  backup_dir: "./data/backups"
  
  # Directory for archived WAL segments
  archive_dir: "./data/wal_archive"
  
  # Enable compression (default: true)
  compress: true
  
  # Keep backups for 30 days (default)
  retention_days: 30
  
  # Optional: custom archive command (for remote storage)
  # archive_command: "aws s3 cp %p s3://my-bucket/wal/%f"
  
  # Optional: custom restore command
  # restore_command: "aws s3 cp s3://my-bucket/wal/%f %p"
```

### WAL Archiving

WAL (Write-Ahead Log) archiving enables point-in-time recovery by preserving transaction logs.

```bash
# Archive current WAL segment manually
veridicaldb wal archive

# List archived segments
veridicaldb wal list
# Output:
# Name                                          Timestamp            Size         LSN
# wal_20260106_150000_000000000001234.log      2026-01-06 15:00:00  16384        4660
# wal_20260106_160000_000000000005678.log      2026-01-06 16:00:00  32768        22136
```

### Restoring from Backup

#### Basic Restore (Latest State)

```bash
# Restore to a new data directory
veridicaldb restore /backups/backup_20260106_153000.tar.gz /data/restored

# Output:
# Restoring from: /backups/backup_20260106_153000.tar.gz
# Target directory: /data/restored
# Restore completed successfully!
#   Base Backup: backup_20260106_153000
#   Files Restored: 42
#   WAL Segments Applied: 0
#   Restored LSN: 5678
#   Duration: 1.2s
```

#### Point-in-Time Recovery (PITR)

Restore to a specific point in time:

```bash
# Restore to a specific time
veridicaldb restore /backups/backup_20260106_120000.tar.gz /data/restored \
  --target-time "2026-01-06T15:30:00Z"

# Restore to a specific WAL position (LSN)
veridicaldb restore /backups/backup_20260106_120000.tar.gz /data/restored \
  --target-lsn 12345

# Specify custom archive directory
veridicaldb restore /backups/backup_20260106_120000.tar.gz /data/restored \
  --target-time "2026-01-06T15:30:00Z" \
  --archive-dir /wal_archive
```

### Backup Strategy Recommendations

#### Daily Full Backup with Continuous WAL Archiving

```bash
# Cron job for daily backup at 2 AM
0 2 * * * /usr/local/bin/veridicaldb backup basebackup --output /backups/daily_$(date +\%Y\%m\%d)

# Archive WAL every 15 minutes
*/15 * * * * /usr/local/bin/veridicaldb wal archive
```

#### Backup Verification

Always verify backups after creation:

```bash
# Verify backup integrity
veridicaldb backup verify /backups/backup_20260106_153000.tar.gz

# Output: Backup verification successful!
```

### Disaster Recovery Workflow

1. **Identify the recovery target** - Determine the point in time or LSN to recover to
2. **Locate the appropriate base backup** - Find the latest backup before your target time
3. **Verify backup integrity** - Run `veridicaldb backup verify`
4. **Perform restore** - Use `veridicaldb restore` with appropriate options
5. **Verify restored data** - Start the database and verify data integrity
6. **Update configuration** - Point your application to the restored database

### Backup Metadata

Each backup includes a metadata file (`*.meta.json`) containing:

```json
{
  "id": "backup_20260106_153000",
  "start_time": "2026-01-06T15:30:00Z",
  "end_time": "2026-01-06T15:30:02Z",
  "start_lsn": 1234,
  "end_lsn": 5678,
  "data_dir": "./data",
  "size": 1048576,
  "checksum": "abc123...",
  "compressed": true,
  "files": {
    "tables/users.dat": "file_checksum..."
  },
  "version": 1
}
```

### Troubleshooting

**"Target directory is not empty"**
- The restore target must be empty or non-existent
- Remove existing files or choose a different directory

**"No archived WAL segments found"**
- Ensure WAL archiving is enabled and running
- Check the archive directory path in configuration

**"Checksum mismatch"**
- Backup may be corrupted during transfer
- Re-download or re-copy the backup file

---

## ❓ Getting Help

- Type `\help` in the VeridicalDB prompt for available commands
- Report issues: https://github.com/JayabrataBasu/VeridicalDB/issues

---

## 📋 System Requirements

- **Linux:** Any 64-bit x86 distribution (Mint 20+, Ubuntu 20.04+, etc.)
- **Windows:** Windows 10 or 11 (64-bit Intel/AMD)
- **Mac:** macOS 11+ with Apple Silicon (M1/M2/M3)

No additional software or dependencies required!
