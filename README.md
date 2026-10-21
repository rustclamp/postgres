<img src="https://docs.rustclamp.com/assets/rustclamp-logo.png" alt="RustClamp logo" width="160">

# rustclamp-postgres

PostgreSQL integration for [RustClamp](https://github.com/rustclamp/rustclamp)
on SQLx's Tokio pool. It owns pool configuration and lifecycle, typed database
qualification, and explicit migration planning. No SQL or driver types leak into
Core or Kernel; SQLx is re-exported (`rustclamp_postgres::sqlx`) for anything driver-specific.
Domain repository ports and error mapping stay in your application.

## Install

Rust 1.96.1+, edition 2024:

```toml
[dependencies]
rustclamp-postgres = "1"
```

## Example

```rust
use rustclamp_postgres::{Migration, Migrations, PoolConfig, Primary};

let db = PoolConfig::<Primary>::new("postgres://localhost/app", 10).connect().await?;
db.health().await?;

let migrations = Migrations::<Primary>::new(vec![Migration {
    version: 1,
    description: "create users",
    sql: "CREATE TABLE users (id BIGINT PRIMARY KEY)",
}])?;
migrations.run(db.pool()).await?;

let tx = db.begin().await?;
// ... queries on &mut *tx ...
tx.commit().await?;
db.close().await;
```

## Main API

- `PoolConfig::<Q>` (`new`, `with_connect_timeout`, `connect`) and `Database<Q>`: `pool`, `begin`, `health`, `close`, `ownership`; `Database::from_pool(pool, Ownership)` adopts an application-owned pool.
- Qualifiers `Primary` and `Analytics` keep multiple databases apart in the type system.
- `Migrations::<Q>::new` validates an ordered plan (`MigrationPlanError`); `run` applies versions not yet present.
- `Database::try_lock(key)` takes a session advisory lock (`pg_try_advisory_lock`) on a dedicated pooled connection and returns `Option<LockGuard>`. `LockGuard::release().await` unlocks and returns the connection to the pool (closing it if the unlock fails); dropping the guard closes the connection, which frees the lock. A held lock never goes back to the pool.
- `LockKey`: from an `i64` as is, or from a name hashed with 64-bit FNV-1a over its UTF-8 bytes (as `i64`), stable across versions.

## Features

| Feature | Adds |
| --- | --- |
| `idempotency` | `PgIdempotencyStore`, a shared store for rustclamp-http's `with_idempotency` (its `idempotency` feature): the claim is one `INSERT ... ON CONFLICT DO NOTHING`, so concurrent retries run the handler once across instances. Add `idempotency_migration(version)` to your plan; call `delete_expired()` from a scheduled job |
| `scheduler` | `Database::leader_lock(key) -> AdvisoryLeader`, a `rustclamp_scheduler::LeaderLock` for `Scheduler::leader_only`: one replica runs the jobs; if its connection dies, another takes over on its next due tick |

```rust
let scheduler = scheduler.leader_only(db.leader_lock("app.scheduler"));
```

`compose.yaml` starts a local PostgreSQL for the integration tests.

Full documentation: <https://docs.rustclamp.com>

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you state otherwise, any
contribution you submit for inclusion is dual licensed as above, without
additional terms or conditions.
