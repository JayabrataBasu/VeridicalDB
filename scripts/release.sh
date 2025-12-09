#!/usr/bin/env bash
set -euo pipefail

# Release script for VeridicalDB
# Builds simplified, user-friendly packages for:
#   - Linux (Mint, Ubuntu, etc.)
#   - Windows (Intel)
#   - macOS Apple Silicon (M1/M2/M3)

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT_DIR="$ROOT_DIR/build/release"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

VERSION=${1:-beta}

# Target platforms: friendly-name:GOOS:GOARCH
targets=(
  "linux:linux:amd64"
  "windows:windows:amd64"
  "mac-silicon:darwin:arm64"
)

echo "=== VeridicalDB Release Build ==="
echo "Version: $VERSION"
echo ""

for t in "${targets[@]}"; do
  IFS=":" read -r FRIENDLY GOOS GOARCH <<< "$t"
  echo "Building: $FRIENDLY ($GOOS/$GOARCH)"
  
  SUFFIX=""
  if [ "$GOOS" = "windows" ]; then
    SUFFIX=".exe"
  fi

  # Build to temp location
  TEMP_BIN="$OUT_DIR/build-temp${SUFFIX}"
  env CGO_ENABLED=0 GOOS=$GOOS GOARCH=$GOARCH go build -ldflags "-s -w" -o "$TEMP_BIN" ./cmd/veridicaldb

  # Create package directory
  PKG_DIR="$OUT_DIR/pkg-temp"
  rm -rf "$PKG_DIR"
  mkdir -p "$PKG_DIR/veridicaldb"
  
  # Copy binary with simple name
  if [ "$GOOS" = "windows" ]; then
    mv "$TEMP_BIN" "$PKG_DIR/veridicaldb/veridicaldb.exe"
  else
    mv "$TEMP_BIN" "$PKG_DIR/veridicaldb/veridicaldb"
  fi
  
  # Copy usage guide
  if [ -f "$ROOT_DIR/USAGE.md" ]; then
    cp "$ROOT_DIR/USAGE.md" "$PKG_DIR/veridicaldb/"
  fi

  # Package with simple name
  pushd "$PKG_DIR" >/dev/null
  if [ "$GOOS" = "windows" ]; then
    ARCHIVE="veridicaldb-${FRIENDLY}.zip"
    zip -rq "$ARCHIVE" veridicaldb
  else
    ARCHIVE="veridicaldb-${FRIENDLY}.tar.gz"
    tar czf "$ARCHIVE" veridicaldb
  fi
  mv "$ARCHIVE" "$OUT_DIR/"
  popd >/dev/null
  rm -rf "$PKG_DIR"
  echo "  -> $ARCHIVE"
done

# Generate checksums
pushd "$OUT_DIR" >/dev/null
echo ""
echo "Generating checksums..."
sha256sum veridicaldb-* > SHA256SUMS
popd >/dev/null

echo ""
echo "=== Build Complete ==="
echo "Artifacts in: $OUT_DIR"
ls -1 "$OUT_DIR"


