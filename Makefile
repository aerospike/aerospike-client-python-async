# Go parameters
hosts ?= ""
host ?= localhost
port ?= 3000
user ?= ""
pass ?= ""
ns ?= "test"

.PHONY: build test install clean stubs
all: lint dev build test install clean

stubs:
	# Generate type stubs and organize them as a Python package
	source aerospike.env && cargo run --bin stub_gen
	@echo "Generated stubs in aerospike_async/aerospike_async.pyi"

lint:
	cargo clippy

dev:
	# Generate a temp wheel & install it as a Python module in local virtual environment
	maturin develop

test:
	python -m pytest tests

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
