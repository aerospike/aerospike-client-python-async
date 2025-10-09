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
	./generate_stubs.sh

lint:
	cargo clippy

dev:
	maturin develop

build:
	maturin build -r

test: dev test-only

test-only:
	python -m unittest discover -s tests -p "*_test.py"

bench: dev
	rm -f aerospike_async/bench.json
	python aerospike_async/benchmarks.py -o bench.json
	pyperf hist aerospike_async/bench.json

clean:
	cargo clean
