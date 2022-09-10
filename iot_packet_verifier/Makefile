# Makefile for Packet-Verifier service to Helium Packet-Router (HPR)

.PHONY: all build compile run release clean dist-clean

all: build

compile: build

build:
	cargo build --all
	cargo fmt --all

test:
	cargo test

run:
	cargo run

release:
	cargo clippy --all
	cargo fmt --all --check
	cargo build --all --release

update:
	cargo update -p helium-proto

clean:
	cargo clean

dist-clean:
	git clean -dXfffffffffff
