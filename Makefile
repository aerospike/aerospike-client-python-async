# Go parameters
hosts ?= ""
host ?= localhost
port ?= 3000
user ?= ""
pass ?= ""
ns ?= "test"

.PHONY: build test install clean stubs local-cargo git-cargo
all: lint dev build test install clean

local-cargo:
	@if [ -f Cargo.toml.local ]; then \
		cp Cargo.toml Cargo.toml.git.bak; \
		cp Cargo.toml.local Cargo.toml; \
		echo "Switched to local Cargo.toml (points to local rust client)"; \
		echo "Original Cargo.toml backed up to Cargo.toml.git.bak"; \
	else \
		echo "Cargo.toml.local not found!"; \
		exit 1; \
	fi

git-cargo:
	@if [ -f Cargo.toml.git.bak ]; then \
		cp Cargo.toml.git.bak Cargo.toml; \
		rm Cargo.toml.git.bak; \
		echo "Switched to git Cargo.toml (points to tls branch)"; \
	else \
		git checkout Cargo.toml; \
		echo "Switched to git Cargo.toml (points to tls branch)"; \
	fi

stubs:
	# Generate type stubs and organize them as a Python package
	# stub_gen.rs will automatically move _aerospike_async_native.pyi to the correct location
	# Suppress warnings from dependencies (aerospike-core) to keep output clean
	source aerospike.env && RUSTFLAGS="-A warnings" cargo run --no-default-features --bin stub_gen 2>&1 | grep -v "warning:.*aerospike-core" || true
	# Post-process stubs to fix issues pyo3_stub_gen can't handle automatically
	@if [ -f python/aerospike_async/__init__.pyi ]; then \
		python python/postprocess_stubs.py python/aerospike_async/__init__.pyi; \
	fi
	@if [ -f python/aerospike_async/_aerospike_async_native.pyi ]; then \
		python python/postprocess_stubs.py python/aerospike_async/_aerospike_async_native.pyi; \
	fi
	@echo "Generated stubs in python/aerospike_async/"

lint:
	cargo clippy

dev:
	# Generate a temp wheel & install it as a Python module in local virtual environment
	maturin develop

test:
	# Clear any stale pytest/bytecode cache that might have incorrect imports
	@python/clean_caches.sh || true
	source aerospike.env && python -m pytest python/tests

dev-test: dev stubs test

build:
	# Generate distributable Python wheel binary and put it in the target folder
	maturin build -r

bench: dev
	rm -f bench.json
	python benchmarks.py -o bench.json
	pyperf hist aerospike_async/bench.json

clean:
	cargo clean
