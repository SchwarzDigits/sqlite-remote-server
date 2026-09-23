// Package migrations embeds the goose migrations into the binary.
package migrations

import "embed"

// FS contains all migrations. Migrations are append-only: a released migration is never changed.
//
//go:embed *.sql
var FS embed.FS
