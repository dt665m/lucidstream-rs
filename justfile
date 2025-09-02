ORG := "dt665m"
PROJECT := "lucidstream-rs"
SEM_VER := `awk -F' = ' '$1=="version"{print $2;exit;}' Cargo.toml`

default:
    @just --list

# Dependencies
deps: deps-rust deps-cargo

deps-rust:
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

deps-cargo:
    cargo install cargo-expand

# Version tagging
tag:
    git tag -a v{{SEM_VER}} -m "v{{SEM_VER}}"

untag:
    git tag -d v{{SEM_VER}}

# Macro Tests
macro-expand:
    if [ -d lucidstream-derive ]; then \
      (cd lucidstream-derive && cargo expand --test 01-parse); \
    else \
      echo "lucidstream-derive not found" >&2; exit 1; \
    fi

# Integration Tests (Postgres)
it-pg: local-pg it-run-pg
    @true

it-run-pg:
    (cd integration-tests && RUST_BACKTRACE=1 RUST_LOG=info cargo test test_all_pg -- --nocapture)

it-pg-bench:
    (cd integration-tests && RUST_BACKTRACE=1 RUST_LOG='integration_test=debug,malory=debug' cargo test --release benchmark -- --nocapture)

# Local Deployment (Docker Compose)
local-pg:
    source docker/.env_local; \
    docker-compose -f docker/docker-compose.yaml up -d postgres adminer; \
    sleep 3

local-down:
    source docker/.env_local; \
    docker-compose -f docker/docker-compose.yaml down

