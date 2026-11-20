package migrations

import "embed"

// Files contains the same versioned SQL used by the migration CLI.
//
//go:embed *.sql
var Files embed.FS
