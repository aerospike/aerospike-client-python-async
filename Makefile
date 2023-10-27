# Go parameters
hosts ?= ""
host ?= localhost
port ?= 3000
user ?= ""
pass ?= ""
ns ?= "test"

.PHONY: build test install clean
all: lint dev build test install clean

lint:
	cargo clippy

dev: lint
	# the following command should be issued manually before dev
	# source .env/bin/activate
	maturin develop

build:
	maturin build -r

test: dev test-only

test-only:
	python3 -m unittest discover -s tests -p "*_test.py"

bench: dev
	rm -f bench.json
	python3 benchmarks.py -o bench.json
	pyperf hist bench.json

bloat:
	cargo bloat --crates --release -n 50

clean:
	cargo clean
