# sqlite-remote-server

[![CI](https://github.com/SchwarzDigits/sqlite-remote-server/actions/workflows/ci.yml/badge.svg)](https://github.com/SchwarzDigits/sqlite-remote-server/actions/workflows/ci.yml)

The page server for [sqlite-remote-vfs](https://github.com/SchwarzDigits/sqlite-remote-vfs). It stores the blocks of
SQLite databases whose VFS runs on a client. With encryption above the VFS on the client, such as SQLite3 Multiple
Ciphers or SQLCipher, the server stores only ciphertext.

Status: works and is tested, not yet in production use. Versions are 0.x: the protocol and the API can still change.

## Features

- **Commits.** The server applies all blocks of a commit in one transaction. A commit must be based on the current
  version, otherwise it is rejected with a version conflict. A commit that a client sends again because the
  acknowledgement was lost is recognized by its commit ID and not applied twice.
- **Leases and fencing.** Opening a database acquires its lease, the exclusive right to commit. If another instance
  holds a valid lease, the open fails unless the client asks for a takeover. Every new lease has a higher epoch, and
  commits with an older epoch are rejected.
- **Login by signature.** The client sends a public key, the server answers with a challenge, and the client signs
  it. The server derives the subject that owns the databases from the public key. A client cannot choose its subject,
  and no setting turns the login off.
- **Change log.** For the last 1024 commits the server keeps the indexes of the blocks each commit changed. A client
  whose local copy is a few commits behind discards only those blocks instead of the whole copy.
- **Deletion.** A client can delete a database it does not have open. The server removes the blocks and the change
  log but keeps a record with the lease epoch and the version, and increases both. If the database is created again,
  epoch and version continue, so no lease on the deleted database can ever commit to the new one.
- **Storage.** In memory for tests, or in PostgreSQL.

## Running

```sh
go install github.com/SchwarzDigits/sqlite-remote-server/cmd/sqlite-remote-server@latest

SQLITE_REMOTE_SERVER_ID=ws://127.0.0.1:8080/v1/ws SQLITE_REMOTE_STORE=memory sqlite-remote-server

docker run -d --rm -p 15432:5432 -e POSTGRES_PASSWORD=vfs -e POSTGRES_USER=vfs -e POSTGRES_DB=vfs postgres:17-alpine
SQLITE_REMOTE_SERVER_ID=ws://127.0.0.1:8080/v1/ws SQLITE_REMOTE_STORE=postgres \
SQLITE_REMOTE_DATABASE_URL='postgres://vfs:vfs@127.0.0.1:15432/vfs?sslmode=disable' sqlite-remote-server
```

The server listens on one port and serves:

| Path | Purpose |
|---|---|
| `/v1/ws` | the WebSocket endpoint for clients |
| `/.well-known/live` | liveness probe, always 200 |
| `/.well-known/ready` | readiness probe, 200 if the store answers within one second |

It logs JSON to stdout and shuts down on SIGINT and SIGTERM: open connections are closed with "going away", so clients
reconnect. With PostgreSQL, the migrations run at start under a session lock, so several instances can start at once.

### TLS

The server speaks plain HTTP and WebSocket. Put it behind a reverse proxy that terminates TLS, and let clients connect
with `wss://`. Do not expose the probes through the proxy.

`SQLITE_REMOTE_SERVER_ID` should be the public URL clients connect to. It is part of what a client signs at login. A
client that compares it with its URL detects a challenge relayed by another server. sqlite-remote-vfs does not compare
it, so clients should use a separate login key for each server.

### Configuration

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `SQLITE_REMOTE_PORT` | no | `8080` | port for the WebSocket and the probes |
| `SQLITE_REMOTE_SERVER_ID` | yes | | public URL, e.g. `wss://vfs.example/v1/ws` |
| `SQLITE_REMOTE_STORE` | yes | | `memory` or `postgres`. There is no default, so a server cannot run on the memory store by accident |
| `SQLITE_REMOTE_DATABASE_URL` | with `postgres` | | PostgreSQL connection string |
| `SQLITE_REMOTE_DB_MAX_CONNS`, `SQLITE_REMOTE_DB_MIN_CONNS` | no | pgxpool default | connection pool size |
| `SQLITE_REMOTE_ALLOWED_ORIGINS` | no | same origin only | comma-separated hosts from which browser pages may connect. `*` matches any part of a host, e.g. `*.example.com` or `127.0.0.1:*` |
| `SQLITE_REMOTE_LOG_LEVEL` | no | `info` | `debug`, `info`, `warn` or `error` |
| `SQLITE_REMOTE_MAX_FRAME_BYTES` | no | 1 MiB | largest frame, at least 65 KiB so that a block of the largest page size fits |
| `SQLITE_REMOTE_MAX_COMMIT_BYTES` | no | 256 MiB | largest commit over all its parts, at least `MAX_FRAME_BYTES` |
| `SQLITE_REMOTE_PING_INTERVAL` | no | `10s` | interval at which idle clients ping |
| `SQLITE_REMOTE_LEASE_TTL` | no | `30s` | time after the last renewal from which a lease can be taken without takeover, at least twice the ping interval |
| `SQLITE_REMOTE_HELLO_TIMEOUT` | no | `5s` | time a client has to complete the login |

### Embedding

Programs that read their configuration differently, for example from the variables of a deployment platform, build
a `server.Config` and call `server.Run`:

```go
cfg := server.DefaultConfig()
cfg.Addr = ":8080"
cfg.ServerID = "wss://vfs.example/v1/ws"
cfg.Store = server.StorePostgres
cfg.DatabaseURL = databaseURL
err := server.Run(ctx, cfg, logger) // returns after ctx is canceled and the server has shut down
```

`Config.Validate` reports an invalid field as a `*server.ConfigError` with the Go field name, so the caller can name
its own setting in the message.

## PostgreSQL storage

A block is stored in parts of at most 1 KB, one row per part, in a table with `fillfactor = 50`. An update in
PostgreSQL writes a new row version. If it fits on the same page (a HOT update), PostgreSQL removes the old version
without VACUUM. Whole 4 KB blocks are stored in the TOAST table instead, where every old version remains until the
next VACUUM: under constant load such a table grew to about 6 GB in two minutes. With block parts its size stays
constant under the same load. This needs only the table option, no setting on the PostgreSQL server.

## Development

| | |
|---|---|
| Tests | `go test ./...`. The PostgreSQL tests also need `SQLITE_REMOTE_TEST_DATABASE_URL`, e.g. `postgres://vfs:vfs@localhost:15432/vfs`, and are skipped without it |
| Lint | `golangci-lint run` |
| Queries | `go tool sqlc generate`. All SQL is in `internal/store/postgres/queries.sql` |
| Protocol | `scripts/sync-proto.sh [path]` copies `proto/` from a sqlite-remote-vfs checkout (default `../sqlite-remote-vfs`) and regenerates `internal/gen`. With `--check` it only compares |

sqlite-remote-vfs owns the protocol. `proto/SOURCE` names the commit it was copied from, and CI checks that `proto/`
still equals it. `internal/protocol/golden_test.go` checks that Go decodes and encodes the golden samples in
`proto/testdata/` byte for byte like the Rust side. The end-to-end tests are in sqlite-remote-vfs and run against
this server.

## Layout

| Path | Content |
|---|---|
| `cmd/sqlite-remote-server` | the command: reads the environment and calls `server.Run` |
| `server` | `Config`, `Validate` and `Run`, the public API |
| `internal/config` | the environment variables of the command |
| `internal/protocol` | WebSocket, login, leases, commits, fetches, change log |
| `internal/store` | the store interface. `memory` and `postgres` implement it, `storetest` checks both against the same contract |
| `internal/platform` | logging, probes, panic recovery, graceful shutdown |
| `migrations` | PostgreSQL migrations, embedded in the binary |
| `proto`, `internal/gen` | the protocol copied from sqlite-remote-vfs and the Go code generated from it |

## License

Apache License 2.0, see [`LICENSE`](LICENSE).
