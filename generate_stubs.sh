#!/bin/bash

# Source environment variables
source aerospike.env

# Generate stubs
cargo run --bin stub_gen

# Create package directory and rename file to __init__.pyi
if [ -f "aerospike_async/aerospike_async.pyi" ]; then
    mkdir -p "aerospike_async/aerospike_async"
    mv "aerospike_async/aerospike_async.pyi" "aerospike_async/__init__.pyi"
    echo "Generated stubs and renamed to __init__.pyi"
else
    echo "Error: Expected file aerospike_async/aerospike_async.pyi not found"
    exit 1
fi