#!/usr/bin/env bash
set -euo pipefail

# Convenience script to build, init (if needed), and start VeridicalDB interactively.
# Usage: ./scripts/start.sh

echo "Checking build..."
if [ ! -x ./build/veridicaldb ]; then
  echo "Building veridicaldb..."
  make build
fi

# Ensure data directory exists
if [ ! -d ./data ]; then
  echo "Initializing database..."
  ./build/veridicaldb init ./data || true
fi

# Start interactive REPL
echo "Starting VeridicalDB (interactive REPL). Use \"\quit\" or Ctrl-D to exit."
exec ./build/veridicaldb --config veridicaldb.yaml
