use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use syn::{LitStr, parse_macro_input};

#[proc_macro]
pub fn embed_migrations(input: TokenStream) -> TokenStream {
    let relative_path = parse_macro_input!(input as LitStr);
    let relative_value = relative_path.value();

    let manifest_dir = match std::env::var("CARGO_MANIFEST_DIR") {
        Ok(value) => value,
        Err(err) => {
            return compile_error(format!(
                "embed_migrations! could not resolve CARGO_MANIFEST_DIR: {err}"
            ));
        }
    };

    let manifest_dir = PathBuf::from(manifest_dir);
    let full_path = manifest_dir.join(&relative_value);
    if !full_path.exists() {
        return compile_error(format!(
            "embed_migrations! path does not exist: {}",
            full_path.display()
        ));
    }
    if !full_path.is_dir() {
        return compile_error(format!(
            "embed_migrations! path is not a directory: {}",
            full_path.display()
        ));
    }

    let canonical_path = match full_path.canonicalize() {
        Ok(path) => path,
        Err(err) => {
            return compile_error(format!(
                "embed_migrations! failed to canonicalize {}: {err}",
                full_path.display()
            ));
        }
    };

    let migrations = match discover_rust_migrations(&canonical_path) {
        Ok(migrations) => migrations,
        Err(err) => return compile_error(err),
    };

    let mut module_tokens = Vec::new();
    let mut registration_tokens = Vec::new();
    let mut used_idents = HashSet::new();

    for migration in migrations {
        let module_ident = unique_module_ident(&migration.script, &mut used_idents);
        let path_lit = lit_str_from_path(&migration.path);
        let script_lit = LitStr::new(&migration.script, Span::call_site());

        module_tokens.push(quote! {
            #[path = #path_lit]
            mod #module_ident;
        });

        registration_tokens.push(quote! {
            migrator.register_rust_migration(
                #script_lit,
                ::schemalane::RustMigrationExecutor::new(|manager| {
                    Box::pin(#module_ident::migration(manager))
                }),
            );
        });
    }

    let migrations_dir_lit = lit_str_from_path(&canonical_path);

    quote! {
        pub mod migrations {
            #(#module_tokens)*

            pub const MIGRATIONS_DIR: &str = #migrations_dir_lit;

            pub fn build_migrator(
                config: ::schemalane::SchemalaneConfig,
            ) -> ::schemalane::SchemalaneMigrator {
                let mut migrator = ::schemalane::SchemalaneMigrator::new(config);
                #(#registration_tokens)*
                migrator
            }

            pub fn runner() -> ::schemalane::EmbeddedRunner {
                ::schemalane::EmbeddedRunner::new(MIGRATIONS_DIR, build_migrator)
            }
        }
    }
    .into()
}

struct RustMigrationFile {
    path: PathBuf,
    script: String,
    version: Vec<u64>,
}

fn discover_rust_migrations(dir: &Path) -> Result<Vec<RustMigrationFile>, String> {
    let mut migrations = Vec::new();

    let read_dir = std::fs::read_dir(dir).map_err(|err| {
        format!(
            "failed to read migrations directory {}: {err}",
            dir.display()
        )
    })?;

    for entry in read_dir {
        let entry = entry
            .map_err(|err| format!("failed to read directory entry in {}: {err}", dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }

        let script = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("non-utf8 migration filename: {}", path.display()))?
            .to_owned();

        let version = parse_rust_migration_filename(&script)?;
        migrations.push(RustMigrationFile {
            path,
            script,
            version,
        });
    }

    migrations.sort_by(|a, b| {
        a.version
            .cmp(&b.version)
            .then_with(|| a.script.cmp(&b.script))
    });
    Ok(migrations)
}

fn parse_rust_migration_filename(file_name: &str) -> Result<Vec<u64>, String> {
    if !file_name.ends_with(".rs") {
        return Err(format!(
            "invalid Rust migration filename '{}': expected .rs extension",
            file_name
        ));
    }

    let stem = &file_name[..file_name.len() - 3];
    let Some(rest) = stem.strip_prefix('V') else {
        return Err(format!(
            "invalid Rust migration filename '{}': expected V<version>__<description>.rs",
            file_name
        ));
    };
    let Some((version_text, description)) = rest.split_once("__") else {
        return Err(format!(
            "invalid Rust migration filename '{}': expected V<version>__<description>.rs",
            file_name
        ));
    };

    if version_text.is_empty() {
        return Err(format!(
            "invalid Rust migration filename '{}': missing version",
            file_name
        ));
    }
    if description.is_empty() {
        return Err(format!(
            "invalid Rust migration filename '{}': missing description",
            file_name
        ));
    }

    let mut version = Vec::new();
    for part in version_text.split(['.', '_']) {
        if part.is_empty() || !part.chars().all(|ch| ch.is_ascii_digit()) {
            return Err(format!(
                "invalid Rust migration filename '{}': invalid version '{}'",
                file_name, version_text
            ));
        }
        let number = part.parse::<u64>().map_err(|err| {
            format!(
                "invalid Rust migration filename '{}': version parse error: {err}",
                file_name
            )
        })?;
        version.push(number);
    }

    if !description
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        return Err(format!(
            "invalid Rust migration filename '{}': invalid description '{}'",
            file_name, description
        ));
    }

    Ok(version)
}

fn unique_module_ident(script: &str, used: &mut HashSet<String>) -> syn::Ident {
    let stem = script.strip_suffix(".rs").unwrap_or(script);
    let mut candidate = sanitize_ident(stem);
    let base = candidate.clone();
    let mut idx = 2usize;

    while !used.insert(candidate.clone()) {
        candidate = format!("{base}_{idx}");
        idx += 1;
    }

    format_ident!("{candidate}")
}

fn sanitize_ident(value: &str) -> String {
    let mut result = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push('_');
        }
    }

    result = result.trim_matches('_').to_owned();
    if result.is_empty() {
        result.push_str("migration");
    }
    if result
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_digit())
    {
        result = format!("m_{result}");
    }

    result
}

fn lit_str_from_path(path: &Path) -> LitStr {
    let as_string = path.to_string_lossy().into_owned();
    LitStr::new(&as_string, Span::call_site())
}

fn compile_error(message: impl AsRef<str>) -> TokenStream {
    let lit = LitStr::new(message.as_ref(), Span::call_site());
    quote! { compile_error!(#lit); }.into()
}
