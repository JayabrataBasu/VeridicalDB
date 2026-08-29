// Package build carries version and build metadata for VeridicalDB.
//
// The values are overridable at link time, e.g.:
//
//	go build -ldflags "\
//	  -X github.com/JayabrataBasu/VeridicalDB/internal/build.Version=v2.1.0 \
//	  -X github.com/JayabrataBasu/VeridicalDB/internal/build.Commit=$(git rev-parse --short HEAD) \
//	  -X github.com/JayabrataBasu/VeridicalDB/internal/build.Date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
//
// This is the single source of truth for the version string; nothing else in the
// tree should declare its own.
package build

var (
	// Version is the release version, e.g. "v2.0.0".
	Version = "v2.0.0"

	// Codename is the release codename, e.g. "Halcyon". May be empty.
	Codename = "Halcyon"

	// Commit is the short git SHA the binary was built from.
	Commit = "unknown"

	// Date is the UTC build timestamp in RFC 3339 form.
	Date = "unknown"
)

// String returns a short human-readable version, e.g. "v2.0.0 (Halcyon)".
func String() string {
	if Codename == "" {
		return Version
	}
	return Version + " (" + Codename + ")"
}

// Full returns the version with commit and build date appended, e.g.
// "v2.0.0 (Halcyon), commit abc1234, built 2026-08-29T12:00:00Z".
func Full() string {
	return String() + ", commit " + Commit + ", built " + Date
}
