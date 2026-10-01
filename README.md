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
  holds a valid lease, the open fails unless the client asks for a takeover. The instance that holds the lease, by its
  instance ID, gets a new lease without takeover, so a client that restarts with the same ID has its database back at
  once. Every new lease has a higher epoch, and commits with an older epoch are rejected. A client that stopped
  without closing keeps its lease for up to five minutes (`LEASE_TTL`); until then another instance needs a takeover.
- **Login by signature.** The client sends a public key, the server answers with a challenge, and the client signs
  it. The server derives the subject that owns the databases from the public key. A client cannot choose its subject,
  and no setting turns the login off.
- **Access tokens (optional).** With a JWKS configured, the server admits only clients with an access token from a
  token service: a JWT signed with EdDSA or ES256, with the configured issuer and audience, and a `cnf` claim
  (RFC 7800) that holds the client's Ed25519 public key. A token is therefore useless without the private key. The
  client sends it in its `Hello`; a missing or invalid token is rejected with `ERROR_CODE_ACCESS_DENIED`. `HelloOk`
  tells the client the token's remaining lifetime, and the client reconnects with a new token before it expires.
  After it has expired, plus the leeway, the server closes the connection at the next request. It still answers
  pings until then, so the client's leases stay renewed. The JWKS is fetched again after its `max-age`, at most five
  minutes, and when a token names an unknown key ID, at most once a minute. If fetching fails, the keys fetched
  before stay in use for up to an hour.
- **Change log.** For the last 1024 commits the server keeps the indexes of the blocks each commit changed. A client
  whose local copy is a few commits behind discards only those blocks instead of the whole copy.
- **Deletion.** A client can delete a database it does not have open. The server removes the blocks and the change
  log but keeps a record with the lease epoch and the version, and increases both. If the database is created again,
  epoch and version continue, so no lease on the deleted database can ever commit to the new one, and a client's
  outdated cache of the deleted database is recognized as outdated.
- **Deletion of unused databases.** A database that no client has opened, read or committed to for 180 days is
  deleted, as if its owner had deleted it. This removes the databases of owners who have lost their key and can
  therefore never delete them. Every open, commit and lease renewal counts as use; a client that is connected renews
  its leases with its pings. The server checks once an hour and logs each deleted database with its subject and name,
  and each sweep with the number deleted.
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
| `SQLITE_REMOTE_REQUIRE_SYNC_REPLICATION` | no | `false` | `true`: refuse to start unless PostgreSQL replicates commits synchronously, see [Durability](#durability) |
| `SQLITE_REMOTE_ALLOWED_ORIGINS` | no | same origin only | comma-separated hosts from which browser pages may connect. `*` matches any part of a host, e.g. `*.example.com` or `127.0.0.1:*` |
| `SQLITE_REMOTE_LOG_LEVEL` | no | `info` | `debug`, `info`, `warn` or `error` |
| `SQLITE_REMOTE_MAX_FRAME_BYTES` | no | 1 MiB | largest frame, at least 65 KiB so that a block of the largest page size fits |
| `SQLITE_REMOTE_MAX_COMMIT_BYTES` | no | 256 MiB | largest commit over all its parts, at least `MAX_FRAME_BYTES` |
| `SQLITE_REMOTE_PING_INTERVAL` | no | `10s` | interval at which idle clients ping |
| `SQLITE_REMOTE_LEASE_TTL` | no | `5m` | time after the last renewal from which a lease can be taken without takeover, at least twice the ping interval. A connected client's lease is written to the store every half TTL: a longer TTL means fewer writes, and a longer wait for another instance after a client stopped without closing |
| `SQLITE_REMOTE_HELLO_TIMEOUT` | no | `5s` | time a client has to complete the login |
| `SQLITE_REMOTE_TOKEN_JWKS_URL` | no | | JWKS of the token service, e.g. `https://tokens.example/.well-known/jwks.json`. Turns on access tokens, see [Features](#features). https, or http on localhost |
| `SQLITE_REMOTE_TOKEN_JWKS_FILE` | no | | the JWKS as a file, read at start, instead of the URL |
| `SQLITE_REMOTE_TOKEN_ISSUER` | with a JWKS | | required `iss` claim |
| `SQLITE_REMOTE_TOKEN_AUDIENCE` | no | `SQLITE_REMOTE_SERVER_ID` | required `aud` claim |
| `SQLITE_REMOTE_TOKEN_LEEWAY` | no | `1m` | allowance for clock differences when checking `exp` and `nbf` |
| `SQLITE_REMOTE_DELETE_UNUSED_AFTER_DAYS` | no | `180` | days without use after which a database is deleted, see [Features](#features). `0` turns this off |

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

A program whose platform sets the same settings under other variable names can reuse the parsing of the command
instead: `config.LoadFrom` takes a function that returns the value for a `SQLITE_REMOTE_*` name, and the program looks
up its own name there.

```go
cfg, err := config.LoadFrom(func(name string) string {
    return os.Getenv(strings.Replace(name, "SQLITE_REMOTE_", "PLATFORM_VFS_", 1))
})
```

## PostgreSQL storage

A block is stored in parts of at most 1 KB, one row per part, in a table with `fillfactor = 50`. An update in
PostgreSQL writes a new row version. If it fits on the same page (a HOT update), PostgreSQL removes the old version
without VACUUM. Whole 4 KB blocks are stored in the TOAST table instead, where every old version remains until the
next VACUUM: under constant load such a table grew to about 6 GB in two minutes. With block parts its size stays
constant under the same load. This needs only the table option, no setting on the PostgreSQL server.

### Durability

A client relies on an acknowledged commit: it does not keep the data elsewhere. The server acknowledges a commit after
PostgreSQL has committed it, and its connections use `synchronous_commit = on` whatever the database's default is. A
commit is therefore in PostgreSQL's write-ahead log on disk before it is acknowledged, and survives a crash of
PostgreSQL, as long as `fsync` is on (its default).

A failover to a replica or a restore from a backup can still lose acknowledged commits, and clients would continue
from an older state without noticing. To avoid that:

- Configure synchronous replication (`synchronous_standby_names`). PostgreSQL then commits only once a standby has
  the commit. The server logs at start whether commits are replicated synchronously.
  `SQLITE_REMOTE_REQUIRE_SYNC_REPLICATION=true` makes it refuse to start otherwise. With synchronous replication, a
  commit waits while no standby is available; the client's commit then times out and is not acknowledged.
- Restore backups with point-in-time recovery to the latest state, not from a periodic dump.

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
| `config` | the environment variables of the command. `LoadFrom` reads them through a function, for programs that receive the settings under other names |
| `internal/protocol` | WebSocket, login, leases, commits, fetches, change log |
| `internal/store` | the store interface. `memory` and `postgres` implement it, `storetest` checks both against the same contract |
| `internal/platform` | logging, probes, panic recovery, graceful shutdown |
| `migrations` | PostgreSQL migrations, embedded in the binary |
| `proto`, `internal/gen` | the protocol copied from sqlite-remote-vfs and the Go code generated from it |

## License

Apache License 2.0, see [`LICENSE`](LICENSE).
