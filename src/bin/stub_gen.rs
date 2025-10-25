use pyo3_stub_gen::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    // Generate stubs for the main aerospike_async module
    let stub = _aerospike_async_native::stub_info()?;
    
    // Override the output directory if specified
    if let Ok(output_dir) = std::env::var("STUB_OUTPUT_DIR") {
        let output_path = PathBuf::from(output_dir);
        // We need to create a new StubInfo with the custom python_root
        let modules = stub.modules.clone();
        let custom_stub = pyo3_stub_gen::StubInfo {
            modules,
            python_root: output_path,
        };
        custom_stub.generate()?;
    } else {
        stub.generate()?;
    }
    
    Ok(())
}