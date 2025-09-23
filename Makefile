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

dev:
	# the following command should be issued manually before dev
	# source .env/bin/activate
	maturin develop

build:
	maturin build -r

test: dev test-only

test-only:
	python -m unittest discover -s tests -p "*_test.py"

bench: dev
	rm -f bench.json
	python benchmarks.py -o bench.json
	pyperf hist bench.json

clean:
	cargo clean
