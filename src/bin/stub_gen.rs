// Copyright 2023-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use pyo3_stub_gen::Result;
use std::path::PathBuf;
use std::fs;

/// The full dotted module path that pyproject.toml declares as
/// `tool.maturin.module-name`. pyo3-stub-gen 0.22 validates that every
/// registered module key matches this prefix; the `#[gen_stub_pyclass]`
/// annotations use the bare `_aerospike_async_native`, so we remap
/// the key here before calling `generate()`.
const FULL_MODULE: &str = "aerospike_async._aerospike_async_native";
const BARE_MODULE: &str = "_aerospike_async_native";

fn main() -> Result<()> {
    let stub = _aerospike_async_native::stub_info()?;

    let output_path = if let Ok(output_dir) = std::env::var("STUB_OUTPUT_DIR") {
        PathBuf::from(output_dir)
    } else {
        stub.python_root.clone()
    };

    let mut custom_stub = stub.clone();
    custom_stub.python_root = output_path.clone();

    // Remap the bare module key to the full dotted path so
    // pyo3-stub-gen's is_pyo3_generated check passes.
    if let Some(mut module) = custom_stub.modules.remove(BARE_MODULE) {
        module.name = FULL_MODULE.to_string();
        module.default_module_name = FULL_MODULE.to_string();
        custom_stub.modules.insert(FULL_MODULE.to_string(), module);
    }

    custom_stub.generate()?;

    // generate() writes to python_root/aerospike_async/_aerospike_async_native/__init__.pyi
    // but we want python_root/aerospike_async/_aerospike_async_native.pyi (flat file).
    let init_stub = output_path
        .join("aerospike_async")
        .join("_aerospike_async_native")
        .join("__init__.pyi");
    let package_stub = output_path
        .join("aerospike_async")
        .join("_aerospike_async_native.pyi");

    if init_stub.exists() {
        fs::rename(&init_stub, &package_stub)?;
        // Remove the now-empty directory
        let dir = init_stub.parent().unwrap();
        let _ = fs::remove_dir(dir);
        eprintln!("Moved {} to {}", init_stub.display(), package_stub.display());
    }

    Ok(())
}
