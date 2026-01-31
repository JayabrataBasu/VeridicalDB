# Contributing to VeridicalDB

Thanks for contributing! A quick guide to running the CI locally and what `make ci` does.

## Running CI locally

- Install Go (1.24 recommended).
- Install developer tools (one-time):

```bash
make dev-setup
```

- Run the full CI steps locally:

```bash
make ci
```

`make ci` performs these steps:

- Downloads and tidies dependencies (`go mod download` / `go mod tidy`).
- Ensures `golangci-lint` v2.7.2 is available; it will attempt `go install` and fall back to downloading a prebuilt binary into `./tools/` if needed.
- Runs `golangci-lint` with project config, `go vet`, and `go test` (coverage saved to `coverage.out`).

Notes:

- The repository CI uses `golangci-lint` v2.7.2 — the Makefile is intentionally pinned to that version for deterministic results.
- On Windows, the Makefile prefers a system-installed `golangci-lint` or will rely on CI runners to provide the appropriate platform binary; for local Windows development, install `golangci-lint` v2.7.2 manually (see <https://github.com/golangci/golangci-lint/releases>).

## Developer Git hooks (optional but recommended)

We provide a pre-commit hook that runs a fast `golangci-lint` check and a quick test to catch regressions before you commit.

- Install the hook (one-time):

```bash
./scripts/install-hook.sh
# or
make install-hook
```

- Remove the hook if you need to:

```bash
make uninstall-hook
```

The hook runs the `scripts/pre-commit.sh` script; if `golangci-lint` isn't installed the hook will tell you to run `make dev-setup`.

## Running a subset

- Fast tests: `go test ./...`.
- Lint only: `make lint`.

If anything in CI behaves unexpectedly on your OS, open an issue or ping maintainers — we can help adapt the Makefile.
