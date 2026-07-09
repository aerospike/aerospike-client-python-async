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

use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyclass;

/// Field ``44`` (WHERE) flag bits for server query explain (phase 1).
///
/// Combine with bitwise OR and pass to :meth:`Client.query_explain` /
/// :meth:`Client.query_explain_blocking` as ``explain_where_flags``.
/// Omit the argument (or pass ``None``) for default explain
/// (``QueryWhereFlags.EXPLAIN`` only).
///
/// Requires Aerospike Server version >= 8.1.3.
// Note: pyo3_stub_gen generates minimal stubs for structs with #[classattr] constants.
// Full stubs are added in postprocess_stubs.py.
#[gen_stub_pyclass(module = "_aerospike_async_native")]
#[pyclass(name = "QueryWhereFlags", module = "_aerospike_async_native")]
pub struct QueryWhereFlags;

#[pymethods]
impl QueryWhereFlags {
    /// Bit 0 encoding selector — must remain clear for v1 wire.
    #[classattr]
    const ENC_VARINT: i64 = aerospike_core::FLAG_ENC_VARINT as i64;

    /// Explain phase — server runs index planner only (always set on explain).
    #[classattr]
    const EXPLAIN: i64 = aerospike_core::FLAG_EXPLAIN as i64;

    /// Reject primary-index fallback on explain when combined with ``EXPLAIN``.
    #[classattr]
    const REQUIRE_INDEX: i64 = aerospike_core::FLAG_REQUIRE_INDEX as i64;

    /// Require field ``21`` index name hint; fail if hint missing or not selected.
    #[classattr]
    const HARD_HINT: i64 = aerospike_core::FLAG_HARD_HINT as i64;
}
