# VeridicalDB Beta Release

This small README describes how to run the beta artifacts distributed with the release.

Quick start (native binary)

Linux / macOS:

```bash
tar xzf veridicaldb-<version>-<os>-<arch>.tar.gz
./veridicaldb --data-dir ./data
```

Windows:

```powershell
# unzip the release
Expand-Archive veridicaldb-<version>-windows-amd64.zip
.\veridicaldb-windows-amd64.exe --data-dir .\data 
```

Docker:

```bash
docker run -p 5432:5432 -v $(pwd)/data:/data veridicaldb:<version>
```

Notes for testers
- The binary is a standalone Go executable — no Go toolchain required.
- Data is written to the directory provided via `--data-dir` (create it and ensure write permissions).
- Check `SHA256SUMS` to verify the downloaded artifact.
- Report issues at the repository's GitHub issues page.