// Package protocol implements the server side of the sqlite-remote protocol over WebSocket. The message types in
// internal/gen are generated from proto/sqlite_remote/v1/sqlite_remote.proto. proto/ is a copy of the definition in
// sqlite-remote-vfs and is updated with scripts/sync-proto.sh.
package protocol
