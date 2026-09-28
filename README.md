# rustclamp-postgres

Optional PostgreSQL integration using SQLx's Tokio pool and transaction APIs.
The package owns PostgreSQL pool configuration, lifecycle ownership, typed
database qualification, and explicit migration planning. It does not add SQL,
driver types, or runtime dependencies to Core or Kernel.

The API keeps SQLx types available to callers that need ecosystem-specific
features. Application code owns domain repository ports and maps driver errors
into its semantic errors.

See [Phase 6 boundary evidence](../rustclamp/docs/adr/0005-phase6-integration-boundaries.md).
