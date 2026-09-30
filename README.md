<img src="https://docs.rustclamp.com/assets/rustclamp-logo.png" alt="RustClamp logo" width="160">

# rustclamp-postgres

PostgreSQL integration for [RustClamp](https://github.com/rustclamp/rustclamp)
on SQLx's Tokio pool. It owns pool configuration and lifecycle, typed database
qualification, and explicit migration planning. No SQL or driver types leak into
Core or Kernel; SQLx is re-exported (`rustclamp_postgres::sqlx`) for anything driver-specific.
Domain repository ports and error mapping stay in your application.

## Install

Not yet published to crates.io; depend on it from git (Rust 1.96.1+, edition 2024):

```toml
[dependencies]
rustclamp-postgres = { git = "https://github.com/rustclamp/postgres" }
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

`compose.yaml` starts a local PostgreSQL for the integration tests.

Full documentation: <https://docs.rustclamp.com>

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you state otherwise, any
contribution you submit for inclusion is dual licensed as above, without
additional terms or conditions.
