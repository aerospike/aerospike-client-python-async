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
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

use crate::CollectionIndexType;
use crate::Filter;
use crate::errors::RustClientError;

/// Server query plan selection inferred from an explain response.
#[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
#[pyclass(from_py_object, module = "_aerospike_async_native")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuerySelection {
    #[pyo3(name = "PRIMARY_INDEX")]
    PrimaryIndex,
    #[pyo3(name = "SECONDARY_INDEX")]
    SecondaryIndex,
    #[pyo3(name = "FILTERED_OUT")]
    FilteredOut,
}

impl From<aerospike_core::query::QuerySelection> for QuerySelection {
    fn from(value: aerospike_core::query::QuerySelection) -> Self {
        match value {
            aerospike_core::query::QuerySelection::PrimaryIndex => QuerySelection::PrimaryIndex,
            aerospike_core::query::QuerySelection::SecondaryIndex => {
                QuerySelection::SecondaryIndex
            }
            aerospike_core::query::QuerySelection::FilteredOut => QuerySelection::FilteredOut,
        }
    }
}

fn core_collection_index_type(cit: &aerospike_core::CollectionIndexType) -> CollectionIndexType {
    match cit {
        aerospike_core::CollectionIndexType::Default => CollectionIndexType::Default,
        aerospike_core::CollectionIndexType::List => CollectionIndexType::List,
        aerospike_core::CollectionIndexType::MapKeys => CollectionIndexType::MapKeys,
        aerospike_core::CollectionIndexType::MapValues => CollectionIndexType::MapValues,
    }
}

/// Result of a server query explain (phase 1).
#[gen_stub_pyclass(module = "_aerospike_async_native")]
#[pyclass(from_py_object, name = "QueryPlan", module = "_aerospike_async_native")]
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub(crate) _as: aerospike_core::QueryPlan,
}

#[gen_stub_pymethods]
#[pymethods]
impl QueryPlan {
    #[getter]
    pub fn selection(&self) -> QuerySelection {
        self._as.selection().into()
    }

    #[getter]
    pub fn namespace(&self) -> &str {
        self._as.namespace()
    }

    #[getter]
    pub fn set_name(&self) -> Option<&str> {
        self._as.set_name()
    }

    #[getter]
    pub fn ael(&self) -> PyResult<String> {
        self._as.ael().map_err(|e| PyErr::from(RustClientError(e)))
    }

    #[getter]
    pub fn index_name(&self) -> Option<&str> {
        self._as.index_name()
    }

    #[getter]
    pub fn index_type(&self) -> CollectionIndexType {
        core_collection_index_type(self._as.index_type())
    }

    #[getter]
    pub fn is_primary_index(&self) -> bool {
        self._as.is_primary_index()
    }

    #[getter]
    pub fn is_secondary_index(&self) -> bool {
        self._as.is_secondary_index()
    }

    #[getter]
    pub fn is_filtered_out(&self) -> bool {
        self._as.is_filtered_out()
    }

    /// Build the execute ``Filter`` for a secondary-index plan.
    pub fn filter_for_execute(&self) -> PyResult<Option<Filter>> {
        match self._as.filter_for_execute() {
            Ok(Some(filter)) => Ok(Some(Filter { _as: filter })),
            Ok(None) => Ok(None),
            Err(e) => Err(PyErr::from(RustClientError(e))),
        }
    }
}
