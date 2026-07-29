fn main() {
    // Re-export pyo3's interpreter cfgs (e.g. `Py_3_13`, `Py_GIL_DISABLED`) so
    // this crate can gate code on the target Python ABI — e.g. `Py_IsFinalizing`
    // in src/completion.rs is only available on Python >= 3.13.
    pyo3_build_config::use_pyo3_cfgs();
}
