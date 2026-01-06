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

## ❓ Getting Help

- Type `\help` in the VeridicalDB prompt for available commands
- Report issues: https://github.com/JayabrataBasu/VeridicalDB/issues

---

## 📋 System Requirements

- **Linux:** Any 64-bit x86 distribution (Mint 20+, Ubuntu 20.04+, etc.)
- **Windows:** Windows 10 or 11 (64-bit Intel/AMD)
- **Mac:** macOS 11+ with Apple Silicon (M1/M2/M3)

No additional software or dependencies required!
