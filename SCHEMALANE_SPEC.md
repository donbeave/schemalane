# Schemalane v1 Specification (Draft)

## 1. Scope

Schemalane v1 is a PostgreSQL-only, forward-only migration toolkit with a Flyway-compatible history table and operational model.

### 1.1 In Scope

- Migration formats:
  - SQL migrations (primary)
  - Rust migrations (for complex logic)
- Usage modes:
  - As a Rust crate
  - As an embedded tool in application binaries
  - As a CLI
  - As a programmatic migrator API
- Commands:
  - `init`
  - `up`
  - `status`
  - `fresh`
- Driver stack:
  - SeaORM APIs over SQLx PostgreSQL driver

### 1.2 Out of Scope (v1)

- MySQL or SQLite support
- `down`, `undo`, `reset`, or `refresh`
- Repeatable (`R`), baseline (`B`), or undo (`U`) migration types

## 2. Command Surface

Schemalane CLI namespace:

- `schemalane migrate init`
- `schemalane migrate up`
- `schemalane migrate status`
- `schemalane migrate fresh`

### 2.1 Common Flags (`up`, `status`, `fresh`)

- `--database-url <postgres://...>`
- `--schema <schema_name>` (default: `public`)
- `--dir <path>` (default: `./migrations`)
- `--history-table <name>` (default: `flyway_schema_history`)
- `--installed-by <name>` (default: current DB user)

### 2.2 Command-Specific Flags

- `schemalane migrate init`
  - `--path <path>` (default: `./migration`)
  - `--force` (overwrite existing scaffold files)
- `schemalane migrate status`
  - `--format table|json` (default: `table`)
  - `--fail-on-pending`
- `schemalane migrate fresh`
  - `--yes` (required)

### 2.3 `init` Scaffold Output

`schemalane migrate init` creates a standalone migration crate with:

- a runnable CLI (`src/main.rs`)
- a reusable migrator builder (`src/lib.rs`)
- SQL and Rust sample migrations in one folder (`./migrations`)
- `embed_migrations!("./migrations")` in `src/lib.rs` for auto Rust migration detection

### 2.4 Embedded Registration

Embedded mode uses macro-based registration:

- `embed_migrations!("<dir>")` scans Rust migration files at compile time
- generates `migrations::build_migrator(config)` and `migrations::MIGRATIONS_DIR`
- generates `migrations::runner()` for shared embedded CLI execution
- avoids manual migration module lists in `src/lib.rs`

## 3. Migration Discovery and Parsing

Schemalane builds one ordered migration stream from SQL and Rust files in the same directory.

### 3.1 SQL Naming Rules

- Required pattern: `V<version>__<description>.sql`
- `<version>` regex: `^[0-9]+([._][0-9]+)*$`
- `<description>` regex: `^[a-z0-9_]+$`
- Display description: underscores converted to spaces

Examples:

- `V1__init.sql`
- `V2_1__add_indexes.sql`
- `V2026.02.24.1__price_histories.sql`

### 3.2 Rust Migration Identity Rules

Rust migration files follow: `V<version>__<description>.rs`

- `<version>` regex: `^[0-9]+([._][0-9]+)*$`
- `<description>` regex: `^[a-z0-9_]+$`
- `script` is the filename
- `checksum` is calculated from Rust file content
- `type = RUST`

Rust migrations participate in the same global version ordering as SQL migrations.

### 3.3 Validation Rules

Startup validation errors (hard fail):

- Invalid filename/metadata format
- Duplicate versions across SQL and Rust migrations
- Duplicate script names
- Non-PostgreSQL URL

## 4. Execution Model

### 4.1 Forward-Only

- All migrations are forward-only.
- To undo a change, create a new higher-version migration.
- No `down`/`undo` operations exist.

### 4.2 SQL Migration Execution

SQL migrations are transactional by default and executed via SeaORM connection APIs:

```rust
let db = manager.get_connection();
let txn = db.begin().await?;
txn.execute_unprepared(sql_text).await?;
txn.commit().await?;
```

Requirements:

- One SQL file may contain multiple SQL statements.
- On failure, rollback when possible.

### 4.3 Rust Migration Execution

- Rust migrations are non-transactional by default.
- Each migration may opt into its own transaction strategy explicitly.

## 5. PostgreSQL Locking

Schemalane acquires a single PostgreSQL advisory lock for the full migration session (`up` and `fresh`) to prevent concurrent runners.

- Acquire lock before reading history and applying migrations.
- Release lock after completion (or on error via cleanup path).

## 6. History Table (Flyway-Compatible)

Default fully-qualified table name:

- `"public"."flyway_schema_history"` (schema configurable)

### 6.1 DDL

```sql
CREATE TABLE IF NOT EXISTS "public"."flyway_schema_history" (
    "installed_rank" INTEGER NOT NULL,
    "version" VARCHAR(50),
    "description" VARCHAR(200) NOT NULL,
    "type" VARCHAR(20) NOT NULL,
    "script" VARCHAR(1000) NOT NULL,
    "checksum" INTEGER,
    "installed_by" VARCHAR(100) NOT NULL,
    "installed_on" TIMESTAMPTZ NOT NULL DEFAULT now(),
    "execution_time" INTEGER NOT NULL,
    "success" BOOLEAN NOT NULL,
    CONSTRAINT "flyway_schema_history_pk" PRIMARY KEY ("installed_rank")
);

CREATE INDEX IF NOT EXISTS "flyway_schema_history_s_idx"
    ON "public"."flyway_schema_history" ("success");

CREATE INDEX IF NOT EXISTS "flyway_schema_history_v_idx"
    ON "public"."flyway_schema_history" ("version");
```

### 6.2 Write Semantics

For every migration attempt:

- Insert one row with:
  - next `installed_rank`
  - migration metadata (`version`, `description`, `type`, `script`)
  - `checksum`
  - `installed_by`
  - `execution_time` in milliseconds
  - `success = true|false`

Failed attempts are recorded (`success = false`) and surfaced in `status`.

## 7. Status State Model

`status` evaluates local migrations and history rows into these states:

- `Success`:
  - Applied row exists with `success = true`
  - Checksum matches current local migration
- `Pending`:
  - Local migration not present in successful history rows
- `Failed`:
  - History row exists with `success = false`
- `Missing`:
  - Successful history row has no corresponding local migration
- `ChecksumMismatch`:
  - Successful history row exists for same migration identity, checksum differs

### 7.1 Drift Definition

Drift is any migration in:

- `Missing`
- `ChecksumMismatch`

## 8. Exit Codes

- `0`: success
- `1`: runtime/config/database error
- `2`: migration validation error
- `3`: drift detected (`Missing` or `ChecksumMismatch`)
- `4`: failed migration present (`success = false`)
- `5`: pending migrations found with `--fail-on-pending`
- `6`: destructive guard violation (`fresh` without `--yes`)

## 9. `fresh` Semantics

`fresh` is destructive and must require `--yes`.

Execution sequence:

1. Acquire advisory lock.
2. Validate migration set.
3. Drop all user tables in target schema (including history table).
4. Recreate `flyway_schema_history`.
5. Execute `up`.
6. Release lock.

`fresh` never drops the PostgreSQL database itself.

## 10. Programmatic API (Minimum)

Minimum API surface (crate mode):

- `init_migration_project(&Path, force: bool) -> Result<InitReport, Error>`
- `Migrator::up(&DatabaseConnection, &Config) -> Result<RunReport, Error>`
- `Migrator::status(&DatabaseConnection, &Config) -> Result<StatusReport, Error>`
- `Migrator::fresh(&DatabaseConnection, &Config) -> Result<RunReport, Error>`

All four usage modes (crate, embedded, CLI, programmatic) share this core engine.
