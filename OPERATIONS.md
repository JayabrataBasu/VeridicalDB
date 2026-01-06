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

Operational tips:
- Verify backups after creation: `veridicaldb backup verify /path/to/backup`
- Ensure WAL archives are continuous and verified (missing segments block PITR beyond missing point)
- Use retention policies to prune old backups and archived WAL segments to control storage costs

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
