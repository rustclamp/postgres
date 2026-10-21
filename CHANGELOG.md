# Changelog

## 1.1.0 - 2026-10-21

- Feature `idempotency`: `PgIdempotencyStore` implements rustclamp-http's `IdempotencyStore` on a `PgPool`, with `idempotency_migration(version)` and `delete_expired()`. Adds an optional dependency on rustclamp-http.
- Add `Database::try_lock(key) -> Option<LockGuard>`, a session advisory lock
  on a dedicated pooled connection, and `LockKey` (an `i64`, or a name hashed
  with FNV-1a 64). `release()` unlocks and returns the connection; drop closes
  it (#8).
- Add feature `scheduler`: `Database::leader_lock(key)`, an
  `rustclamp_scheduler::LeaderLock` for `Scheduler::leader_only`
  (rustclamp/scheduler#11, #8).

## 1.0.0 - 2026-10-19

- First release on crates.io.
