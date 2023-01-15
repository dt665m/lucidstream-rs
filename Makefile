# Path and Variables
SHELL := /bin/bash
ORG := dt665m 
PROJECT := lucidstream-rs 
REPO := github.com/${ORG}/${PROJECT}
ROOT_DIR := $(CURDIR)
SEM_VER := $(shell awk -F' = ' '$$1=="version"{print $$2;exit;}' Cargo.toml)

ES_CONTAINER := eventstore
ifeq ($(shell uname), Darwin)
	ifeq ($(shell uname -m), arm64)
		PLATFORM := aarch64-apple-darwin
        ES_CONTAINER := eventstore-arm 
	else ifeq 
		PLATFORM := x86_64-apple-darwin
	endif
else ifeq ($(UNAME), Linux)
	PLATFORM := x86_64-unknown-linux-gnu
endif

deps: deps-rust deps-cargo

deps-rust:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

deps-cargo:
	cargo install cargo-expand

tag:
	git tag -a v${SEM_VER} -m "v${SEM_VER}"

untag:
	git tag -d v${SEM_VER}

###########################################################
### Macro Tests 

macro-expand:
	cd ${ROOT_DIR}/lucidstream-derive ; \
	cargo expand --test 01-parse

###########################################################
### Integration Tests

it-ges: local-es it-run local-down

it-pg: local-pg it-run-pg local-down

it-run-es: 
	cd ${ROOT_DIR}/integration_tests ; \
	RUST_BACKTRACE=1 RUST_LOG=info cargo test test_all_es -- --nocapture

it-run-pg: 
	cd ${ROOT_DIR}/integration_tests ; \
	RUST_BACKTRACE=1 RUST_LOG=info cargo test test_all_pg -- --nocapture

it-ges-bench: local-es 
	cd ${ROOT_DIR}/integration_tests ; \
	RUST_BACKTRACE=1 RUST_LOG='integration_test=debug,malory=debug' cargo test --release benchmark -- --nocapture

###########################################################
### Local Deployment

# https://github.com/EventStore/EventStore/pkgs/container/eventstore/7973829?tag=20.6.1-alpha.0.69-arm64v8
# https://github.com/EventStore/EventStore/pkgs/container/eventstore/11006179?tag=21.10.0-alpha-arm64v8
local-es:
	source ${ROOT_DIR}/docker/.env_local; \
	docker-compose -f ${ROOT_DIR}/docker/docker-compose.yaml up -d eventstore-arm; \
	sleep 3

local-pg:
	source ${ROOT_DIR}/docker/.env_local; \
	docker-compose -f ${ROOT_DIR}/docker/docker-compose.yaml up -d postgres adminer; \
	sleep 3

local-down:
	source ${ROOT_DIR}/docker/.env_local; \
	docker-compose -f ${ROOT_DIR}/docker/docker-compose.yaml down

.PHONY: deps deps-rust it-ges local-es local-down 

