# Schemalane

PostgreSQL-first, forward-only migrations with SQL as default and optional Rust migrations.

Repository layout:

- Cargo workspace root
- library crate: `schemalane-core`
- CLI crate: `schemalane-cli`
- proc-macro crate: `schemalane-macros`

## Commands

Schemalane CLI supports:

- `schemalane migrate init`
- `schemalane migrate up`
- `schemalane migrate status`
- `schemalane migrate fresh`

## Bootstrap A Migration Crate

Generate a migration crate (SeaORM-style):

```sh
cargo run -p schemalane-cli -- migrate init --path ./migration
```

This creates:

- `migration/Cargo.toml`
- `migration/src/main.rs`
- `migration/src/lib.rs`
- `migration/migrations/V1__create_cake_table.sql`
- `migration/migrations/V2__seed_cake_table.rs`

Run it from your parent project:

```sh
cargo run --manifest-path ./migration/Cargo.toml -- --database-url "$DATABASE_URL" up
```

## Direct CLI Usage

```sh
cargo run -p schemalane-cli -- migrate --database-url "$DATABASE_URL" up
```

Use a migration crate path (SeaORM-style):

```sh
cargo run -p schemalane-cli -- migrate -d ./migration up
```

```sh
cargo run -p schemalane-cli -- migrate --database-url "$DATABASE_URL" status
```

```sh
cargo run -p schemalane-cli -- migrate --database-url "$DATABASE_URL" fresh --yes
```

## Notes

- SQL files: `V<version>__<description>.sql`
- Rust files: `V<version>__<description>.rs`
- SQL runs in a transaction by default.
- Rust migration transaction mode is controlled by executor registration.
- `src/lib.rs` uses `embed_migrations!("./migrations")` to auto-register Rust migration files by script name.
- generated `src/main.rs` is minimal and uses shared CLI via `embedded::migrations::runner().run().await` (backed by `schemalane-cli`).
