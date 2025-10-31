use pyo3_stub_gen::Result;
use std::path::PathBuf;
use std::fs;

fn main() -> Result<()> {
    // `stub_info` is a function defined by `define_stub_info_gatherer!` macro.
    let stub = _aerospike_async_native::stub_info()?;

    // Override the output directory if specified
    let output_path = if let Ok(output_dir) = std::env::var("STUB_OUTPUT_DIR") {
        PathBuf::from(output_dir)
    } else {
        stub.python_root.clone()
    };

    // Generate stubs with the output directory
    let modules = stub.modules.clone();
    let custom_stub = pyo3_stub_gen::StubInfo {
        modules,
        python_root: output_path.clone(),
    };
    custom_stub.generate()?;

    // Move _aerospike_async_native.pyi from python/ to python/aerospike_async/
    // This is needed because pyo3_stub_gen creates it at the root, but we need it in the package
    let root_stub = output_path.join("_aerospike_async_native.pyi");
    let package_stub = output_path.join("aerospike_async").join("_aerospike_async_native.pyi");

    if root_stub.exists() {
        // Ensure the package directory exists
        if let Some(parent) = package_stub.parent() {
            fs::create_dir_all(parent)?;
        }
        // Move the file to the package directory
        fs::rename(&root_stub, &package_stub)?;
        eprintln!("Moved {} to {}", root_stub.display(), package_stub.display());
    }

    // Clean up any incorrectly nested directories (from when STUB_OUTPUT_DIR=python/aerospike_async)
    let double_nested = output_path.join("aerospike_async").join("aerospike_async");
    if double_nested.exists() {
        fs::remove_dir_all(&double_nested)?;
        eprintln!("Removed incorrectly nested directory: {}", double_nested.display());
    }

    Ok(())
}
