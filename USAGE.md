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

## More details & advanced topics

For advanced usage (TLS configuration, backups & PITR, WAL archiving, restore procedures, and security best practices), see `OPERATIONS.md`.

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
