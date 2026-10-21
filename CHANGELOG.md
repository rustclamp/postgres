# Changelog

## Unreleased

- Feature `idempotency`: `PgIdempotencyStore` implements rustclamp-http's `IdempotencyStore` on a `PgPool`, with `idempotency_migration(version)` and `delete_expired()`. Adds an optional dependency on rustclamp-http.

## 1.0.0 - 2026-10-19

- First release on crates.io.
