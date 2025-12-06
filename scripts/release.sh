#!/usr/bin/env bash
set -euo pipefail

# Simple release script to cross-compile VeridicalDB for multiple platforms
# Produces packaged artifacts in the build/release directory and a SHA256SUMS file.

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT_DIR="$ROOT_DIR/build/release"
mkdir -p "$OUT_DIR"

VERSION=${1:-v0.0.0-beta}

targets=(
  "linux:amd64"
  "linux:arm64"
  "darwin:amd64"
  "darwin:arm64"
  "windows:amd64"
)

echo "Building release $VERSION"

for t in "${targets[@]}"; do
  IFS=":" read -r GOOS GOARCH <<< "$t"
  echo "- target: $GOOS/$GOARCH"
  BINARY_NAME=veridicaldb
  SUFFIX=""
  if [ "$GOOS" = "windows" ]; then
    SUFFIX=".exe"
  fi
  OUT_BIN="$OUT_DIR/${BINARY_NAME}-${GOOS}-${GOARCH}${SUFFIX}"

  env CGO_ENABLED=0 GOOS=$GOOS GOARCH=$GOARCH go build -ldflags "-s -w" -o "$OUT_BIN" ./cmd/veridicaldb

  # package
  pushd "$OUT_DIR" >/dev/null
  PKG_NAME="veridicaldb-${VERSION}-${GOOS}-${GOARCH}"
  mkdir -p "$PKG_NAME"
  cp "${BINARY_NAME}-${GOOS}-${GOARCH}${SUFFIX}" "$PKG_NAME/"
  # include README and sample config if available
  if [ -f "$ROOT_DIR/README.BETA.md" ]; then
    cp "$ROOT_DIR/README.BETA.md" "$PKG_NAME/"
  fi
  if [ -d "$ROOT_DIR/sample-config" ]; then
    cp -r "$ROOT_DIR/sample-config" "$PKG_NAME/"
  fi

  if [ "$GOOS" = "windows" ]; then
    zip -r "${PKG_NAME}.zip" "$PKG_NAME" >/dev/null
    rm -rf "$PKG_NAME"
  else
    tar czf "${PKG_NAME}.tar.gz" "$PKG_NAME"
    rm -rf "$PKG_NAME"
  fi
  popd >/dev/null
done

pushd "$OUT_DIR" >/dev/null
echo "Generating checksums"
sha256sum * > SHA256SUMS
popd >/dev/null

echo "Release artifacts written to $OUT_DIR"
echo "Done."
