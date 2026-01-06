# VeridicalDB — Operations & Administration Guide

This document contains advanced operational guidance for running and managing VeridicalDB in production environments, including TLS configuration, backup & point-in-time recovery (PITR), WAL archiving, restore procedures, and security best practices.

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

This section provides operational details for backups, WAL archiving, and point-in-time recovery.

### Overview

- Base backups capture a consistent copy of the data directory and record the starting WAL LSN.
- WAL archiving stores WAL segments so you can replay transactions after a base backup to reach a specific time/LSN.
- PITR combines a base backup and subsequent WAL segments to restore the database to any point between the base backup start and the latest archived WAL.

### Creating Base Backups

A base backup is a complete copy of your database at a specific point in time.

```bash
# Create backup with default settings (compressed, stored in data/backups/)
veridicaldb backup basebackup

# Create backup to a specific location
veridicaldb backup basebackup --output /backups/mydb_backup
```

Notes:
- Backups include a metadata file (`*.meta.json`) which records `start_lsn`, `end_lsn`, timestamps, file checksums and a backup ID.
- Consider scheduling regular full backups (daily/weekly) and retaining them as per your retention policy.

### WAL Archiving

WAL (Write-Ahead Log) archiving preserves transaction logs for PITR.

Manual archiving:
```bash
veridicaldb wal archive
```

Automated/remote archiving:
- Configure `backup.archive_command` in `veridicaldb.yaml` to call `aws s3 cp %p s3://bucket/wal/%f` or similar.
- Ensure the archive command returns non-zero on failures so the archiver can retry/report.

### Restoring and PITR

Basic restore:
```bash
veridicaldb restore /backups/backup_YYYYMMDD_HHMMSS.tar.gz /data/restored
```

PITR:
```bash
veridicaldb restore /backups/backup_YYYYMMDD_HHMMSS.tar.gz /data/restored \
  --target-time "2026-01-06T15:30:00Z"
# or
veridicaldb restore /backups/backup_YYYYMMDD_HHMMSS.tar.gz /data/restored \
  --target-lsn 12345
```

Restoring from remote archives:
```bash
# Use a restore command to fetch missing WAL from remote storage (placeholders: %f filename, %p destination)
veridicaldb restore /backups/backup_YYYYMMDD_HHMMSS.tar.gz /data/restored \
  --restore-command "aws s3 cp s3://my-bucket/wal/%f %p"
```
- You can also set `backup.restore_command` in `veridicaldb.yaml` to avoid passing the flag.
- Pair this with `backup.archive_command` (e.g., `aws s3 cp %p s3://bucket/wal/%f`) to enable remote WAL archiving.

Operational tips:
- Verify backups after creation: `veridicaldb backup verify /path/to/backup`
- Ensure WAL archives are continuous and verified (missing segments block PITR beyond missing point)
- Use retention policies to prune old backups and archived WAL segments to control storage costs

### Retention and Pruning

VeridicalDB provides automated retention management for backups and WAL archives.

**Manual pruning:**
```bash
# Dry run to see what would be deleted
veridicaldb backup prune --dry-run

# Delete old backups keeping 7 most recent and 30 days of history
veridicaldb backup prune --keep-backups 7 --keep-days 30

# More aggressive: keep only 3 backups
veridicaldb backup prune --keep-backups 3 --keep-days 7
```

**Configuration-based retention:**
```yaml
backup:
  retention_days: 30          # days to keep backups
```

**Scheduling automated pruning:**
- Use cron or systemd timers to run `veridicaldb backup prune` daily/weekly
- Example cron entry: `0 3 * * * /usr/local/bin/veridicaldb backup prune --keep-backups 7 --keep-days 30`

### S3/Remote Archiving

For production deployments, archive WAL segments and backups to remote storage.

**Configure in `veridicaldb.yaml`:**
```yaml
backup:
  archive_command: "aws s3 cp %p s3://my-bucket/wal/%f"
  restore_command: "aws s3 cp s3://my-bucket/wal/%f %p"
```

**Using MinIO or S3-compatible storage:**
```yaml
backup:
  archive_command: "aws s3 cp %p s3://my-bucket/wal/%f --endpoint-url http://minio:9000"
  restore_command: "aws s3 cp s3://my-bucket/wal/%f %p --endpoint-url http://minio:9000"
```

**Built-in S3 archiver (programmatic use):**
```go
import "github.com/JayabrataBasu/VeridicalDB/pkg/backup"

archiver, _ := backup.NewS3Archiver(&backup.S3Config{
    Bucket:   "my-bucket",
    Prefix:   "wal/",
    Region:   "us-west-2",
    Endpoint: "http://minio:9000", // optional for S3-compatible
})

// Upload WAL segment
archiver.ArchiveWALSegment(ctx, "/path/to/wal.log")

// Generate commands for config
fmt.Println(archiver.GenerateArchiveCommand())
fmt.Println(archiver.GenerateRestoreCommand())
```

### Monitoring and Metrics

VeridicalDB exposes backup metrics in Prometheus format for monitoring.

**Key metrics:**
- `veridicaldb_backup_last_timestamp_seconds` - Unix timestamp of last backup
- `veridicaldb_backup_duration_seconds` - Duration of last backup
- `veridicaldb_backup_size_bytes` - Size of last backup
- `veridicaldb_archive_lsn` - LSN of last archived WAL
- `veridicaldb_archive_lag_seconds` - Estimated archive lag
- `veridicaldb_backup_errors_total` - Total backup errors
- `veridicaldb_verify_success` - Last verification success (1=success, 0=failure)

**Recommended alerts:**
- Alert if `veridicaldb_backup_last_timestamp_seconds` is older than 25 hours (missed daily backup)
- Alert if `veridicaldb_archive_lag_seconds` > 300 (archive falling behind)
- Alert if `veridicaldb_backup_errors_total` increases
- Alert if `veridicaldb_verify_success` == 0

---

## 🧰 Troubleshooting & Maintenance

- "Target directory is not empty" — make sure the restore target is empty or pick a fresh directory.
- "No archived WAL segments found" — verify `backup.archive_dir` and archive process is active.
- "Checksum mismatch" — backup may be corrupted; re-run and verify; check transfer integrity.

---

## 📋 Operational Playbook (Summary)

1. Schedule regular base backups
2. Enable WAL archiving and verify uploads
3. Monitor archive verification and retention
4. Practice restores periodically to validate procedures

---

For developer-level details (formats, metadata fields, and implementation notes), see the repository documentation and the `pkg/backup` package sources.
