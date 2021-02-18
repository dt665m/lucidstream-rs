# Path and Variables
SHELL := /bin/bash
ORG := dt665m 
PROJECT := lucidstream-rs 
REPO := github.com/${ORG}/${PROJECT}
ROOT_DIR := $(CURDIR)
SEM_VER := $(shell awk -F' = ' '$$1=="version"{print $$2;exit;}' lucidstream/Cargo.toml)

deps: deps-rust

deps-rust:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

tag:
	git tag -a v${SEM_VER} -m "v${SEM_VER}"

untag:
	git tag -d v${SEM_VER}


###########################################################
### Integration Tests

it-ges: local-es it-run local-down

it-run: 
	cd ${ROOT_DIR}/integration-tests ; \
	RUST_BACKTRACE=1 RUST_LOG=debug cargo test test_all -- --nocapture

it-ges-bench: local-es 
	cd ${ROOT_DIR}/integration-tests ; \
	RUST_BACKTRACE=1 RUST_LOG='integration_test=debug,malory=debug' cargo test --release benchmark -- --nocapture

###########################################################
### Local Deployment

local-es:
	source ${ROOT_DIR}/docker/.env_local; \
	docker-compose -f ${ROOT_DIR}/docker/docker-compose.yaml up -d eventstore; \
	sleep 3

local-down:
	source ${ROOT_DIR}/docker/.env_local; \
	docker-compose -f ${ROOT_DIR}/docker/docker-compose.yaml down

.PHONY: deps deps-rust it-ges local-es local-down 

