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

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};

use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyImportError, PyIndexError, PyTypeError, PyValueError};
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyDict, PyList};
use pyo3::{prelude::*, Borrowed, IntoPyObjectExt};

use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};
use pyo3_stub_gen::{PyStubType, TypeInfo};

use numpy::{dtype, PyArrayDescrMethods, PyReadonlyArray1, PyUntypedArray, PyUntypedArrayMethods};



    /**********************************************************************************
     *
     * Record
     *
     **********************************************************************************/

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1000, module = "_aerospike_async_native")]
    pub struct Record {
        pub(crate) _as: aerospike_core::Record,
        /// Lazily-cached Python dict for the ``bins`` property.
        /// Avoids re-cloning and re-converting on every access.
        pub(crate) cached_bins: Option<Py<PyAny>>,
        /// Lazily-cached Python list for the ``results`` property.
        pub(crate) cached_results: Option<Py<PyAny>>,
    }

    impl Clone for Record {
        fn clone(&self) -> Self {
            Record {
                _as: self._as.clone(),
                // Don't carry caches across clones; the new owner
                // will lazily rebuild them if needed.
                cached_bins: None,
                cached_results: None,
            }
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Record {
        pub fn bin(&self, name: &str) -> Option<Py<PyAny>> {
            let b = self._as.bins.get(name);
            b.map(|v| {
                let v: PythonValue = v.to_owned().into();
                Python::attach(|py| v.into_pyobject(py).unwrap().unbind())
            })
        }

        #[getter]
        pub fn get_bins(&mut self, py: Python<'_>) -> Py<PyAny> {
            if let Some(ref cached) = self.cached_bins {
                return cached.clone_ref(py);
            }
            let dict = PyDict::new(py);
            for (k, v) in &self._as.bins {
                let pv: PythonValue = v.clone().into();
                let py_val = pv.into_pyobject(py).unwrap();
                dict.set_item(k, py_val).unwrap();
            }
            let py_obj: Py<PyAny> = dict.into_any().unbind();
            self.cached_bins = Some(py_obj.clone_ref(py));
            py_obj
        }

        /// Positional results, one slot per op in request order.
        ///
        /// Use ``record.results`` when the request issued multiple ops and you
        /// need to address each result by its op index — e.g. a chain that
        /// modifies a bin and reads it back in the same execute. Slots for
        /// ops that produced no value carry Python ``None``.
        ///
        /// For by-name access, prefer ``record.bins`` (cheaper for the common
        /// case of one op per bin).
        #[getter]
        pub fn get_results(&mut self, py: Python<'_>) -> Option<Py<PyAny>> {
            if let Some(ref cached) = self.cached_results {
                return Some(cached.clone_ref(py));
            }
            let results = self._as.results.as_ref()?;
            let list = PyList::empty(py);
            for v in results {
                let pv: PythonValue = v.clone().into();
                let py_val = pv.into_pyobject(py).unwrap();
                list.append(py_val).unwrap();
            }
            let py_obj: Py<PyAny> = list.into_any().unbind();
            self.cached_results = Some(py_obj.clone_ref(py));
            Some(py_obj)
        }

        /// Return the positional result for the *i*-th op in the request,
        /// or ``None`` if *i* is out of range.
        ///
        /// Equivalent to ``record.results[i]`` but without bounds-error noise:
        /// out-of-range returns Python ``None`` (matching the in-range
        /// nil-result encoding). Use this when the request size is dynamic
        /// and the caller wants a uniform optional shape.
        pub fn operation_result(&self, i: usize) -> Option<Py<PyAny>> {
            self._as.operation_result(i).map(|v| {
                let pv: PythonValue = v.to_owned().into();
                Python::attach(|py| pv.into_pyobject(py).unwrap().unbind())
            })
        }

        #[getter]
        pub fn get_generation(&self) -> Option<u32> {
            Some(self._as.generation)
        }

        #[getter]
        pub fn get_ttl(&self) -> Option<u32> {
            self._as.time_to_live().map(|v| v.as_secs() as u32)
        }

        #[getter]
        pub fn get_key(&self) -> Option<Key> {
            self._as.key.as_ref().map(|k| Key { _as: k.clone() })
        }

        fn __str__(&self) -> PyResult<String> {
            Ok(format!("{}", self))
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("Record({})", s))
        }
    }

    impl fmt::Display for Record {
        fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
            write!(f, "generation: {}", self._as.generation)?;
            write!(f, ", ttl: ")?;
            let _ = match self._as.time_to_live() {
                None => "None".fmt(f),
                Some(duration) => duration.as_secs().fmt(f),
            };
            write!(f, ", key: {:?}", self._as.key)?;
            write!(f, ", bins: {{")?;
            for (i, (k, v)) in self._as.bins.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "'{}': {}", k, v)?;
            }
            write!(f, "}}")?;
            Ok(())
        }
    }

    /**********************************************************************************
     *
     * Key
     *
     **********************************************************************************/

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1000, module = "_aerospike_async_native")]
    #[derive(Clone)]
    pub struct Key {
        pub(crate) _as: aerospike_core::Key,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Key {
        #[new]
        fn new(namespace: &str, set: &str, key: PythonValue) -> Self {
            // Pass key value directly to core client - supports strings, bytes, integers, and None
            let _as = aerospike_core::Key::new(namespace, set, key.into()).unwrap();
            Key { _as }
        }

        /// Fast-path Key constructor for the bench/per-op hot loop where
        /// the caller has an integer user_key. Skips the Python ``str(int)``
        /// conversion AND the ``PythonValue`` enum dispatch by going
        /// directly to ``Value::String(key.to_string())``. Matches the
        /// existing-cluster on-disk representation (string-typed user keys)
        /// — switch to ``from_int`` instead if you want int-typed user
        /// keys (a different on-server keyspace).
        ///
        /// Per-op cost drops from ~2 µs (PyO3 PythonValue dispatch +
        /// Python str()) to ~500 ns (positional PyO3 call + Rust string
        /// alloc).
        #[staticmethod]
        pub fn from_int_user_key(namespace: &str, set: &str, key: i64) -> Self {
            let value = aerospike_core::Value::String(key.to_string());
            let _as = aerospike_core::Key::new(namespace, set, value).unwrap();
            Key { _as }
        }

        #[staticmethod]
        /// Create a Key from a namespace, set, and digest (20-byte hash).
        /// The digest can be provided as bytes or a hex-encoded string.
        pub fn key_with_digest(namespace: &str, set: &str, digest: &Bound<'_, PyAny>) -> PyResult<Self> {
            let digest_bytes: Vec<u8> = if let Ok(bytes) = digest.extract::<Vec<u8>>() {
                bytes
            } else if let Ok(hex_str) = digest.extract::<String>() {
                hex::decode(&hex_str).map_err(|e| PyValueError::new_err(format!("Invalid hex digest: {}", e)))?
            } else if let Ok(byte_array) = digest.extract::<&[u8]>() {
                byte_array.to_vec()
            } else {
                return Err(PyTypeError::new_err("Digest must be bytes, bytearray, or hex string"));
            };

            if digest_bytes.len() != 20 {
                return Err(PyValueError::new_err(format!(
                    "Digest must be exactly 20 bytes, got {} bytes",
                    digest_bytes.len()
                )));
            }

            let mut digest_array = [0u8; 20];
            digest_array.copy_from_slice(&digest_bytes);

            let _as = aerospike_core::Key {
                namespace: namespace.to_string(),
                set_name: set.to_string(),
                user_key: None,
                digest: digest_array,
            };

            Ok(Key { _as })
        }

        #[getter]
        pub fn get_namespace(&self) -> String {
            self._as.namespace.clone()
        }

        #[getter]
        pub fn get_set_name(&self) -> String {
            self._as.set_name.clone()
        }

        #[getter(value)]
        pub fn get_value(&self) -> Option<PythonValue> {
            // Return key value as-is (preserves integer, string, bytes, etc.)
            match &self._as.user_key {
                Some(v) => {
                    let pv: PythonValue = v.clone().into();
                    Some(pv)
                }
                None => None,
            }
        }

        #[getter]
        pub fn get_digest(&self) -> Option<String> {
            Some(hex::encode(self._as.digest))
        }

        fn __richcmp__(&self, other: Key, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => self._as.digest == other._as.digest,
                CompareOp::Ne => self._as.digest != other._as.digest,
                _ => false,
            }
        }

        fn __str__(&self) -> PyResult<String> {
            Ok(format!("{}", self._as))
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("Key({})", s))
        }

        pub fn __copy__(&self) -> Self {
            self.clone()
        }

        pub fn __deepcopy__(&self, _memo: &Bound<PyDict>) -> Self {
            // fast bitwise copy instead of python's pickling process
            self.clone()
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Blob
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, sequence, module = "_aerospike_async_native")]
    #[derive(Debug, Clone)]
    pub struct Blob {
        v: Vec<u8>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Blob {
        #[new]
        pub fn new(v: Vec<u8>) -> Self {
            Blob { v }
        }

        #[getter]
        pub fn get_value(&self) -> Vec<u8> {
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, b: Vec<u8>) {
            self.v = b
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::Blob(self.v.clone()).as_string()
        }

        fn __getitem__(&mut self, idx: usize) -> PyResult<u8> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"));
            }
            Ok(self.v[idx])
        }

        fn __setitem__(&mut self, idx: usize, v: u8) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"));
            }
            self.v[idx] = v;
            Ok(())
        }

        fn __hash__(&self) -> u64 {
            let mut s = DefaultHasher::new();
            self.v.hash(&mut s);
            s.finish()
        }

        fn __richcmp__<'a>(&self, other: &Bound<'a, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: Result<Blob, _> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<Vec<u8>> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                }
                CompareOp::Ne => {
                    let l: Result<Blob, _> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<Vec<u8>> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                }
                _ => false,
            }
        }

        fn __add__(&self, other: &Bound<PyAny>) -> PyResult<Blob> {
            // Handle Blob + Blob
            if let Ok(other_blob) = other.extract::<Blob>() {
                let mut result = self.v.clone();
                result.extend_from_slice(&other_blob.v);
                return Ok(Blob::new(result));
            }

            // Handle Blob + Vec<u8>
            if let Ok(other_vec) = other.extract::<Vec<u8>>() {
                let mut result = self.v.clone();
                result.extend_from_slice(&other_vec);
                return Ok(Blob::new(result));
            }

            Err(PyTypeError::new_err("unsupported operand type(s) for +: 'Blob' and other type"))
        }

        fn __mul__(&self, other: &Bound<PyAny>) -> PyResult<Blob> {
            // Handle Blob * int
            if let Ok(count) = other.extract::<i32>() {
                if count < 0 {
                    return Err(PyValueError::new_err("can't multiply Blob by negative number"));
                }
                let mut result = Vec::new();
                for _ in 0..count {
                    result.extend_from_slice(&self.v);
                }
                return Ok(Blob::new(result));
            }

            Err(PyTypeError::new_err("unsupported operand type(s) for *: 'Blob' and other type"))
        }

        fn __iadd__(&mut self, other: &Bound<PyAny>) -> PyResult<()> {
            // Handle Blob += Blob
            if let Ok(other_blob) = other.extract::<Blob>() {
                self.v.extend_from_slice(&other_blob.v);
                return Ok(());
            }

            // Handle Blob += Vec<u8>
            if let Ok(other_vec) = other.extract::<Vec<u8>>() {
                self.v.extend_from_slice(&other_vec);
                return Ok(());
            }

            Err(PyTypeError::new_err("unsupported operand type(s) for +=: 'Blob' and other type"))
        }

        fn __imul__(&mut self, other: &Bound<PyAny>) -> PyResult<()> {
            // Handle Blob *= int
            if let Ok(count) = other.extract::<i32>() {
                if count < 0 {
                    return Err(PyValueError::new_err("can't multiply Blob by negative number"));
                }
                let original = self.v.clone();
                self.v.clear();
                for _ in 0..count {
                    self.v.extend_from_slice(&original);
                }
                return Ok(());
            }

            Err(PyTypeError::new_err("unsupported operand type(s) for *=: 'Blob' and other type"))
        }

        fn __delitem__(&mut self, idx: usize) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"));
            }
            self.v.remove(idx);
            Ok(())
        }

        fn __len__(&self) -> PyResult<usize> {
            Ok(self.v.len())
        }
    }

    impl fmt::Display for Blob {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    // impl From<Blob> for PythonValue {
    //     fn from(input: Blob) -> Self {
    //         PythonValue::Blob(input.v.clone())
    //     }
    // }

    // impl Into<PythonValue> for Blob {
    //     fn into(self) -> PythonValue {
    //         PythonValue::Blob(self.v)
    //     }
    // }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Map
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, sequence, module = "_aerospike_async_native")]
    #[derive(Debug, Clone)]
    pub struct Map {
        v: HashMap<PythonValue, PythonValue>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Map {
        #[new]
        pub fn new(v: HashMap<PythonValue, PythonValue>) -> Self {
            Map { v }
        }

        #[getter]
        pub fn get_value(&self) -> HashMap<PythonValue, PythonValue> {
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, b: HashMap<PythonValue, PythonValue>) {
            self.v = b
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::HashMap(self.v.clone()).as_string()
        }

        // TODO: Change HashMap into BTreeMap and use that
        // This requires Rust Client implementation first
        // fn __hash__(&self) -> u64 {
        //     let mut s = DefaultHasher::new();
        //     self.v.hash(&mut s);
        //     s.finish()
        // }

        fn __richcmp__<'a>(&self, other: &Bound<'a, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: Result<Map, _> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<HashMap<PythonValue, PythonValue>> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                }
                CompareOp::Ne => {
                    let l: Result<Map, _> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<HashMap<PythonValue, PythonValue>> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                }
                _ => false,
            }
        }

        fn __str__(&self) -> PyResult<String> {
            // Convert HashMap to JSON-like string format
            let mut items = Vec::new();
            for (k, v) in &self.v {
                let key_str = match k {
                    PythonValue::String(s) => format!("\"{}\"", s),
                    _ => format!("{:?}", k),
                };
                let val_str = match v {
                    PythonValue::String(s) => format!("\"{}\"", s),
                    PythonValue::Int(i) => i.to_string(),
                    PythonValue::Bool(b) => b.to_string(),
                    PythonValue::Float(f) => f.to_string(),
                    PythonValue::Nil => "None".to_string(),
                    _ => format!("{:?}", v),
                };
                items.push(format!("{}: {}", key_str, val_str));
            }
            Ok(format!("{{{}}}", items.join(", ")))
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("Map({})", s))
        }
    }

    impl fmt::Display for Map {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    // impl From<HashMap> for PythonValue {
    //     fn from(input: HashMap) -> Self {
    //         PythonValue::HashMap(input.v.clone())
    //     }
    // }

    // impl Into<PythonValue> for HashMap {
    //     fn into(self) -> PythonValue {
    //         PythonValue::HashMap(self.v)
    //     }
    // }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  List
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    fn format_python_value(value: &PythonValue) -> String {
        match value {
            PythonValue::String(s) => format!("\"{}\"", s),
            PythonValue::Int(i) => i.to_string(),
            PythonValue::Bool(b) => if *b { "True".to_string() } else { "False".to_string() },
            PythonValue::Float(f) => f.to_string(),
            PythonValue::Nil => "None".to_string(),
            PythonValue::List(l) => {
                let mut items = Vec::new();
                for item in l {
                    items.push(format_python_value(item));
                }
                format!("[{}]", items.join(", "))
            },
            PythonValue::HashMap(h) => {
                let mut items = Vec::new();
                let mut sorted_entries: Vec<_> = h.iter().collect();
                sorted_entries.sort_by_key(|(k, _)| format_python_value(k));
                for (k, v) in sorted_entries {
                    items.push(format!("{}: {}", format_python_value(k), format_python_value(v)));
                }
                format!("{{{}}}", items.join(", "))
            },
            PythonValue::OrderedMap(pairs) => {
                let items: Vec<_> = pairs.iter()
                    .map(|(k, v)| format!("{}: {}", format_python_value(k), format_python_value(v)))
                    .collect();
                format!("{{{}}}", items.join(", "))
            },
            _ => format!("{:?}", value),
        }
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, sequence, module = "_aerospike_async_native")]
    #[derive(Debug, Clone)]
    pub struct List {
        v: Vec<PythonValue>,
        index: usize,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl List {
        #[new]
        pub fn new(v: Vec<PythonValue>) -> Self {
            List { v, index: 0 }
        }

        #[getter]
        pub fn get_value(&self) -> Vec<PythonValue> {
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, geo: Vec<PythonValue>) {
            self.v = geo
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::List(self.v.clone()).as_string()
        }

        fn __str__(&self) -> PyResult<String> {
            // Convert internal representation to Python list format
            let mut items = Vec::new();
            for item in &self.v {
                let item_str = format_python_value(item);
                items.push(item_str);
            }
            Ok(format!("[{}]", items.join(", ")))
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("List({})", s))
        }

        fn __getitem__(&mut self, idx: usize) -> PyResult<PythonValue> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"));
            }
            Ok(self.v[idx].clone())
        }

        fn __setitem__(&mut self, idx: usize, v: PythonValue) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"));
            }
            self.v[idx] = v;
            Ok(())
        }

        fn __delitem__(&mut self, idx: usize) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"))
            }
            self.v.remove(idx);
            Ok(())
        }

        fn __concat__(&self, mut other: List) -> PyResult<List> {
            let mut new_list = self.v.clone();
            new_list.append(&mut other.v);
            Ok(List { v: new_list, index: 0 })
        }

        fn __inplace_concat__(&mut self, mut other: List) -> PyResult<List> {
            self.v.append(&mut other.v);
            Ok(self.clone())
        }

        fn __repeat__(&self, times: usize) -> PyResult<List> {
            let og = self.v.clone();
            let len = self.v.len();
            let new_list: Vec<_> = og.into_iter().cycle().take(len * times).collect();
            Ok(List { v: new_list, index: 0 })
        }

        fn __inplace_repeat__(&mut self, times: usize) -> PyResult<List> {
            self.__repeat__(times)
        }
        fn __hash__(&self) -> u64 {
            let mut s = DefaultHasher::new();
            self.v.hash(&mut s);
            s.finish()
        }

        fn __len__(&self) -> usize {
            self.v.len()
        }
        fn __richcmp__<'a>(&self, other: &Bound<'a, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: Result<List, _> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<Vec<PythonValue>> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                }
                CompareOp::Ne => {
                    let l: Result<List, _> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<Vec<PythonValue>> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                }
                _ => false,
            }
        }

        fn __iter__(&self) -> Self {
            self.clone()
        }

        fn __next__<'a>(&mut self, py: Python<'a>) -> Option<Py<PyAny>> {
            let res = self.v.get(self.index);
            self.index += 1;
            res.map(|v| v.clone().into_pyobject(py).unwrap().unbind())
        }
    }

    impl fmt::Display for List {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    // impl From<List> for PythonValue {
    //     fn from(input: List) -> Self {
    //         PythonValue::List(input.v.clone())
    //     }
    // }

    // impl Into<PythonValue> for List {
    //     fn into(self) -> PythonValue {
    //         PythonValue::List(self.v)
    //     }
    // }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  GeoJSON
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, module = "_aerospike_async_native")]
    #[derive(Debug, Clone)]
    pub struct GeoJSON {
        v: String,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl GeoJSON {
        #[new]
        pub fn new<'a>(py: Python<'a>, v: &Bound<'a, PyAny>) -> PyResult<Self> {
            // Accept both String and dict inputs
            if let Ok(s) = v.extract::<String>() {
                return Ok(GeoJSON { v: s });
            }

            // If it's already a GeoJSON object, extract its value
            if let Ok(geo) = v.extract::<GeoJSON>() {
                return Ok(geo);
            }

            // Try to extract as dict and serialize to JSON
            if let Ok(dict) = v.cast::<PyDict>() {
                // Use Python's json module to serialize the dict
                let json_module = PyModule::import(py, "json")?;
                let json_dumps = json_module.getattr("dumps")?;
                let json_string: String = json_dumps.call1((dict,))?.extract()?;
                return Ok(GeoJSON { v: json_string });
            }


            Err(PyTypeError::new_err(
                "GeoJSON constructor requires a string, dict, or GeoJSON object"
            ))
        }

        #[getter]
        pub fn get_value(&self) -> String {
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, geo: String) {
            self.v = geo
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::GeoJSON(self.v.clone()).as_string()
        }

        fn __richcmp__<'a>(&self, other: &Bound<'a, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: Result<GeoJSON, _> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<String> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                }
                CompareOp::Ne => {
                    let l: Result<GeoJSON, _> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<String> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                }
                _ => false,
            }
        }

        fn __str__(&self) -> PyResult<String> {
            Ok(self.v.clone())
        }

        fn __repr__(&self) -> PyResult<String> {
            Ok(format!("GeoJSON({})", self.v))
        }
    }

    impl fmt::Display for GeoJSON {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  HLL
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, sequence, module = "_aerospike_async_native")]
    #[derive(Debug, Clone)]
    pub struct HLL {
        v: Vec<u8>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl HLL {
        #[new]
        pub fn new(v: Vec<u8>) -> Self {
            HLL { v }
        }

        #[getter]
        pub fn get_value(&self) -> Vec<u8> {
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, hll: Vec<u8>) {
            self.v = hll
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::HLL(self.v.clone()).as_string()
        }

        fn __richcmp__<'a>(&self, other: &Bound<'a, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: Result<HLL, _> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<Vec<u8>> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                }
                CompareOp::Ne => {
                    let l: Result<HLL, _> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<Vec<u8>> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                }
                _ => false,
            }
        }
    }

    impl fmt::Display for HLL {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Vector
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Element type of a :class:`Vector`, controlling how each element is stored
    /// on the wire.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum VectorElementType {
        /// IEEE 754 half precision, carried as raw 16-bit patterns.
        #[pyo3(name = "FLOAT16")]
        Float16,
        /// 32-bit signed integer.
        #[pyo3(name = "INT32")]
        Int32,
        /// 32-bit IEEE 754 float (the default).
        #[pyo3(name = "FLOAT32")]
        Float32,
        /// 64-bit IEEE 754 double.
        #[pyo3(name = "FLOAT64")]
        Float64,
    }

    #[pymethods]
    impl VectorElementType {
        fn __richcmp__(
            &self,
            other: &VectorElementType,
            op: pyo3::class::basic::CompareOp,
        ) -> pyo3::PyResult<bool> {
            match op {
                pyo3::class::basic::CompareOp::Eq => Ok(self == other),
                pyo3::class::basic::CompareOp::Ne => Ok(self != other),
                _ => Err(pyo3::exceptions::PyNotImplementedError::new_err(
                    "Only == and != comparisons are supported",
                )),
            }
        }

        fn __hash__(&self) -> u64 {
            let mut hasher = DefaultHasher::new();
            self.hash(&mut hasher);
            hasher.finish()
        }
    }

    impl From<&VectorElementType> for aerospike_core::VectorElementType {
        fn from(input: &VectorElementType) -> Self {
            match input {
                VectorElementType::Float16 => aerospike_core::VectorElementType::Float16,
                VectorElementType::Int32 => aerospike_core::VectorElementType::Int32,
                VectorElementType::Float32 => aerospike_core::VectorElementType::Float32,
                VectorElementType::Float64 => aerospike_core::VectorElementType::Float64,
            }
        }
    }

    impl From<&aerospike_core::VectorElementType> for VectorElementType {
        fn from(input: &aerospike_core::VectorElementType) -> Self {
            match input {
                aerospike_core::VectorElementType::Float16 => VectorElementType::Float16,
                aerospike_core::VectorElementType::Int32 => VectorElementType::Int32,
                aerospike_core::VectorElementType::Float32 => VectorElementType::Float32,
                aerospike_core::VectorElementType::Float64 => VectorElementType::Float64,
            }
        }
    }

    /// Whether `numpy` has been imported, via a `sys.modules` lookup that avoids
    /// triggering numpy's C-API init (which panics when numpy is not installed).
    fn numpy_is_imported(py: Python<'_>) -> bool {
        py.import("sys")
            .and_then(|sys| sys.getattr("modules"))
            .and_then(|modules| modules.contains("numpy"))
            .unwrap_or(false)
    }

    /// Build a core `Vector` from a numpy array, inferring the element type from
    /// its `dtype`. Returns `Ok(None)` if `data` is not a numpy array.
    fn vector_from_numpy(
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        element_type: Option<VectorElementType>,
    ) -> PyResult<Option<aerospike_core::Vector>> {
        // A numpy array can only exist if numpy is imported; skipping otherwise
        // keeps the list path working without numpy.
        if !numpy_is_imported(py) {
            return Ok(None);
        }

        let Ok(untyped) = data.cast::<PyUntypedArray>() else {
            return Ok(None);
        };

        if untyped.ndim() != 1 {
            return Err(PyTypeError::new_err(format!(
                "Vector requires a 1-D numpy array, got {}-D",
                untyped.ndim()
            )));
        }

        let dt = untyped.dtype();

        let detected = if dt.is_equiv_to(&dtype::<f32>(py)) {
            VectorElementType::Float32
        } else if dt.is_equiv_to(&dtype::<f64>(py)) {
            VectorElementType::Float64
        } else if dt.is_equiv_to(&dtype::<i32>(py)) {
            VectorElementType::Int32
        } else if dt.is_equiv_to(&dtype::<half::f16>(py)) {
            VectorElementType::Float16
        } else {
            return Err(PyTypeError::new_err(format!(
                "Unsupported numpy dtype {dt} for Vector; expected float16, int32, float32, or float64"
            )));
        };

        if let Some(requested) = element_type {
            if requested != detected {
                let requested_core: aerospike_core::VectorElementType = (&requested).into();
                let detected_core: aerospike_core::VectorElementType = (&detected).into();
                return Err(PyTypeError::new_err(format!(
                    "element_type={requested_core} does not match numpy array dtype {dt} (detected {detected_core})"
                )));
            }
        }

        let core = match detected {
            VectorElementType::Float32 => {
                let arr: PyReadonlyArray1<f32> = data.extract()?;
                aerospike_core::Vector::float32(arr.as_array().iter().copied().collect())
            }
            VectorElementType::Float64 => {
                let arr: PyReadonlyArray1<f64> = data.extract()?;
                aerospike_core::Vector::float64(arr.as_array().iter().copied().collect())
            }
            VectorElementType::Int32 => {
                let arr: PyReadonlyArray1<i32> = data.extract()?;
                aerospike_core::Vector::int32(arr.as_array().iter().copied().collect())
            }
            VectorElementType::Float16 => {
                let arr: PyReadonlyArray1<half::f16> = data.extract()?;
                aerospike_core::Vector::float16(arr.as_array().iter().map(|x| x.to_bits()).collect())
            }
        };

        Ok(Some(core))
    }

    /// A dense numeric vector for vector similarity search, stored in a bin with
    /// the ``VECTOR`` particle type.
    ///
    /// Construct one from a list of numbers, optionally specifying the element
    /// type (default :attr:`VectorElementType.FLOAT32`)::
    ///
    ///     Vector([0.12, 0.98, -0.34])
    ///     Vector([1, 2, 3], VectorElementType.INT32)
    ///
    /// A 1-D ``numpy`` array is also accepted; its ``dtype`` selects the element
    /// type, and ``element_type`` (if given) must match it. ``FLOAT16`` requires
    /// a ``numpy.float16`` array::
    ///
    ///     import numpy as np
    ///     Vector(np.array([0.12, 0.98, -0.34], dtype=np.float32))
    ///     Vector(np.array([1, 2, 3], dtype=np.float16))
    ///
    /// Read values with :attr:`value` (a Python list) or :attr:`numpy_value`
    /// (a typed ``numpy`` array); ``FLOAT16`` is only readable via
    /// :attr:`numpy_value`.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, module = "_aerospike_async_native")]
    #[derive(Debug, Clone)]
    pub struct Vector {
        pub(crate) v: aerospike_core::Vector,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Vector {
        #[new]
        #[pyo3(signature = (data, element_type=None))]
        pub fn new(
            py: Python<'_>,
            data: &Bound<'_, PyAny>,
            element_type: Option<VectorElementType>,
        ) -> PyResult<Self> {
            // If handed an existing Vector, copy it (mirrors GeoJSON).
            if let Ok(existing) = data.extract::<Vector>() {
                return Ok(existing);
            }

            if let Some(core) = vector_from_numpy(py, data, element_type)? {
                return Ok(Vector { v: core });
            }

            let et = element_type.unwrap_or(VectorElementType::Float32);
            let core = match et {
                VectorElementType::Float32 => {
                    let d: Vec<f32> = data.extract().map_err(|_| {
                        PyTypeError::new_err("FLOAT32 Vector requires a list of numbers")
                    })?;
                    aerospike_core::Vector::float32(d)
                }
                VectorElementType::Float64 => {
                    let d: Vec<f64> = data.extract().map_err(|_| {
                        PyTypeError::new_err("FLOAT64 Vector requires a list of numbers")
                    })?;
                    aerospike_core::Vector::float64(d)
                }
                VectorElementType::Int32 => {
                    let d: Vec<i32> = data.extract().map_err(|_| {
                        PyTypeError::new_err("INT32 Vector requires a list of integers")
                    })?;
                    aerospike_core::Vector::int32(d)
                }
                VectorElementType::Float16 => {
                    return Err(PyTypeError::new_err(
                        "FLOAT16 Vector requires a numpy.float16 array",
                    ));
                }
            };

            Ok(Vector { v: core })
        }

        /// The element type of this vector.
        #[getter]
        pub fn get_element_type(&self) -> VectorElementType {
            (&self.v.element_type()).into()
        }

        /// Number of elements (dimensions) in this vector.
        #[getter]
        pub fn get_dimensions(&self) -> usize {
            self.v.dimensions()
        }

        /// The vector's elements as a plain Python list.
        ///
        /// Raises :class:`TypeError` for ``FLOAT16``; use :attr:`numpy_value`.
        #[getter]
        pub fn get_value<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            match self.v.data() {
                aerospike_core::VectorData::Float16(_) => Err(PyTypeError::new_err(
                    "FLOAT16 Vector values are not available as a Python list; \
                     use the `.numpy_value` property instead",
                )),
                aerospike_core::VectorData::Int32(d) => d.clone().into_bound_py_any(py),
                aerospike_core::VectorData::Float32(d) => d.clone().into_bound_py_any(py),
                aerospike_core::VectorData::Float64(d) => d.clone().into_bound_py_any(py),
            }
        }

        /// The vector's elements as a typed ``numpy`` array (``dtype`` matching
        /// the element type). Requires numpy. The only way to read ``FLOAT16``.
        #[getter]
        pub fn get_numpy_value<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            // Fail cleanly if numpy is missing rather than panicking in the crate.
            py.import("numpy").map_err(|_| {
                PyImportError::new_err(
                    "the `numpy_value` property requires numpy; \
                     install it with `pip install numpy` (or `aerospike_async[numpy]`)",
                )
            })?;
            let arr = match self.v.data() {
                aerospike_core::VectorData::Float16(d) => {
                    let values: Vec<half::f16> =
                        d.iter().map(|&bits| half::f16::from_bits(bits)).collect();
                    numpy::PyArray1::from_vec(py, values).into_any()
                }
                aerospike_core::VectorData::Int32(d) => {
                    numpy::PyArray1::from_vec(py, d.clone()).into_any()
                }
                aerospike_core::VectorData::Float32(d) => {
                    numpy::PyArray1::from_vec(py, d.clone()).into_any()
                }
                aerospike_core::VectorData::Float64(d) => {
                    numpy::PyArray1::from_vec(py, d.clone()).into_any()
                }
            };
            Ok(arr)
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::Vector(self.v.clone()).as_string()
        }

        fn __len__(&self) -> usize {
            self.v.dimensions()
        }

        fn __richcmp__(&self, other: &Bound<'_, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    if let Ok(o) = other.extract::<Vector>() {
                        return self.v == o.v;
                    }
                    false
                }
                CompareOp::Ne => {
                    if let Ok(o) = other.extract::<Vector>() {
                        return self.v != o.v;
                    }
                    true
                }
                _ => false,
            }
        }

        fn __str__(&self) -> String {
            self.v.to_string()
        }

        fn __repr__(&self) -> String {
            self.v.to_string()
        }
    }

    impl fmt::Display for Vector {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  SpecialValue (CDT range / value boundaries)
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Markers for Aerospike CDT map/list range and value operations.
    /// These use dedicated wire particles, distinct from Python ``float('inf')`` or the ``"*"`` string.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, name = "SpecialValue", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum SpecialValue {
        /// Null particle boundary (e.g. unbounded start of a key range).
        #[pyo3(name = "NULL")]
        Null,
        /// Positive infinity particle (e.g. unbounded end of a value range).
        #[pyo3(name = "INFINITY")]
        Infinity,
        /// Wildcard particle for value matching.
        #[pyo3(name = "WILDCARD")]
        Wildcard,
    }

    #[pymethods]
    impl SpecialValue {
        fn __richcmp__(
            &self,
            other: &SpecialValue,
            op: pyo3::class::basic::CompareOp,
        ) -> pyo3::PyResult<bool> {
            match op {
                pyo3::class::basic::CompareOp::Eq => Ok(self == other),
                pyo3::class::basic::CompareOp::Ne => Ok(self != other),
                _ => Ok(false),
            }
        }

        fn __hash__(&self) -> u64 {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            self.hash(&mut hasher);
            hasher.finish()
        }

        fn __repr__(&self) -> String {
            match self {
                SpecialValue::Null => "SpecialValue.NULL".to_string(),
                SpecialValue::Infinity => "SpecialValue.INFINITY".to_string(),
                SpecialValue::Wildcard => "SpecialValue.WILDCARD".to_string(),
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  PythonValue
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    // Container for bin values stored in the Aerospike database.
    #[derive(Debug, Clone, PartialEq, Eq)]
    #[allow(clippy::upper_case_acronyms)]
    pub enum PythonValue {
        /// Empty value.
        Nil,
        /// Boolean value.
        Bool(bool),
        /// Integer value. All integers are represented as 64-bit numerics in Aerospike.
        Int(i64),
        /// Floating point value. All floating point values are stored in 64-bit IEEE-754 format in
        /// Aerospike. Aerospike server v3.6.0 and later support double data type.
        Float(ordered_float::OrderedFloat<f64>),
        /// String value.
        String(String),
        /// Byte array value.
        Blob(Vec<u8>),
        /// List data type is an ordered collection of values. Lists can contain values of any
        /// supported data type. List data order is maintained on writes and reads.
        List(Vec<PythonValue>),
        /// Map data type is a collection of key-value pairs. Each key can only appear once in a
        /// collection and is associated with a value. Map keys and values can be any supported data
        /// type.
        HashMap(HashMap<PythonValue, PythonValue>),
        /// Ordered map preserving key order from the server (K-ordered / KV-ordered maps).
        /// Stored as Vec of pairs so insertion order into PyDict yields sorted keys.
        OrderedMap(Vec<(PythonValue, PythonValue)>),
        /// GeoJSON data type are JSON formatted strings to encode geospatial information.
        GeoJSON(String),

        /// HLL value
        HLL(Vec<u8>),

        /// Dense numeric vector (see :class:`Vector`).
        Vector(aerospike_core::Vector),

        /// CDT boundary markers (see :class:`SpecialValue`).
        CdtSpecial(SpecialValue),
    }

    #[allow(clippy::derived_hash_with_manual_eq)]
    impl Hash for PythonValue {
        fn hash<H: Hasher>(&self, state: &mut H) {
            match *self {
                PythonValue::Nil => {
                    let v: Option<u8> = None;
                    v.hash(state);
                }
                PythonValue::Bool(ref val) => val.hash(state),
                PythonValue::Int(ref val) => val.hash(state),
                PythonValue::Float(ref val) => val.hash(state),
                PythonValue::String(ref val) | PythonValue::GeoJSON(ref val) => val.hash(state),
                PythonValue::Blob(ref val) | PythonValue::HLL(ref val) => val.hash(state),
                PythonValue::List(ref val) => val.hash(state),
                PythonValue::HashMap(_) | PythonValue::OrderedMap(_) => {
                    panic!("Maps cannot be used as map keys.")
                }
                PythonValue::Vector(_) => panic!("Vectors cannot be used as map keys."),
                PythonValue::CdtSpecial(ref s) => s.hash(state),
            }
        }
    }

    impl PythonValue {
        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            match *self {
                PythonValue::Nil => "<null>".to_string(),
                PythonValue::Int(ref val) => val.to_string(),
                PythonValue::Bool(ref val) => val.to_string(),
                PythonValue::Float(ref val) => val.to_string(),
                PythonValue::String(ref val) => val.to_string(),
                PythonValue::GeoJSON(ref val) => format!("GeoJSON('{}')", val),
                PythonValue::Blob(ref val) => format!("{:?}", val),
                PythonValue::HLL(ref val) => format!("HLL('{:?}')", val),
                PythonValue::Vector(ref val) => val.to_string(),
                PythonValue::List(ref val) => format!("{:?}", val),
                PythonValue::HashMap(ref val) => format!("{:?}", val),
                PythonValue::OrderedMap(ref val) => format!("{:?}", val),
                PythonValue::CdtSpecial(s) => match s {
                    SpecialValue::Null => "SpecialValue.NULL".to_string(),
                    SpecialValue::Infinity => "SpecialValue.INFINITY".to_string(),
                    SpecialValue::Wildcard => "SpecialValue.WILDCARD".to_string(),
                },
            }
        }
    }

    impl fmt::Display for PythonValue {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    impl<'py> IntoPyObject<'py> for PythonValue {
        type Target = PyAny; // the Python type
        type Output = Bound<'py, Self::Target>; // in most cases this will be `Bound`
        type Error = std::convert::Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                PythonValue::Nil => Ok(py.None().into_bound(py)),
                PythonValue::Bool(b) => Ok(PyBool::new(py, b).into_bound_py_any(py).unwrap()),
                PythonValue::Int(i) => Ok(i.into_pyobject(py).map(|v| v.into_any()).unwrap()),
                PythonValue::Float(f) => Ok(f.into_pyobject(py).map(|v| v.into_any()).unwrap()),
                PythonValue::String(s) => Ok(s.into_pyobject(py).map(|v| v.into_any()).unwrap()),
                PythonValue::Blob(b) => Ok(PyBytes::new(py, &b).into_any()),
                PythonValue::List(l) => {
                    let py_list = PyList::empty(py);
                    for item in l {
                        let py_item = item.into_pyobject(py).unwrap();
                        py_list.append(py_item).unwrap();
                    }
                    Ok(py_list.into_any())
                }
                PythonValue::HashMap(h) => {
                    let py_dict = PyDict::new(py);
                    for (k, v) in h {
                        let py_key = k.into_pyobject(py).unwrap();
                        let py_val = v.into_pyobject(py).unwrap();
                        py_dict.set_item(py_key, py_val).unwrap();
                    }
                    Ok(py_dict.into_any())
                }
                PythonValue::OrderedMap(pairs) => {
                    let py_dict = PyDict::new(py);
                    for (k, v) in pairs {
                        let py_key = k.into_pyobject(py).unwrap();
                        let py_val = v.into_pyobject(py).unwrap();
                        py_dict.set_item(py_key, py_val).unwrap();
                    }
                    Ok(py_dict.into_any())
                }
                PythonValue::GeoJSON(s) => {
                    let geo = GeoJSON { v: s };
                    Ok(geo.into_pyobject(py).map(|v| v.into_any()).unwrap())
                }
                PythonValue::HLL(b) => Ok(HLL::new(b).into_pyobject(py).map(|v| v.into_any()).unwrap()),
                PythonValue::Vector(v) => {
                    let vec = Vector { v };
                    Ok(vec.into_pyobject(py).map(|v| v.into_any()).unwrap())
                }
                PythonValue::CdtSpecial(s) => {
                    Ok(s.into_pyobject(py).map(|v| v.into_any()).unwrap())
                }
            }
        }
    }

    impl<'a, 'py> FromPyObject<'a, 'py> for PythonValue {
        type Error = PyErr;

        fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
            // Handle None first - check if the object is None
            if obj.is_none() {
                return Ok(PythonValue::Nil);
            }

            if let Ok(sv) = <SpecialValue as FromPyObject>::extract(obj) {
                return Ok(PythonValue::CdtSpecial(sv));
            }

            let b: PyResult<bool> = obj.extract();
            if let Ok(b) = b {
                return Ok(PythonValue::Bool(b));
            }

            // Try to extract as integer - handle both i64 and large u64 values
            // First try i64 (most common case)
            let i: PyResult<i64> = obj.extract();
            if let Ok(i) = i {
                return Ok(PythonValue::Int(i));
            }

            // For u64 values, convert to i64 (UInt has been removed from Rust core)
            // Values > i64::MAX will overflow, but this matches Rust core behavior
            let ui: PyResult<u64> = obj.extract();
            if let Ok(ui) = ui {
                // Convert u64 to i64 (may overflow for values > i64::MAX)
                return Ok(PythonValue::Int(ui as i64));
            }

            let f1: PyResult<f64> = obj.extract();
            if let Ok(f1) = f1 {
                return Ok(PythonValue::Float(ordered_float::OrderedFloat(f1)));
            }

            let s: PyResult<String> = obj.extract();
            if let Ok(s) = s {
                return Ok(PythonValue::String(s));
            }

            // Try to extract as bytearray
            if let Ok(ba) = obj.cast::<PyByteArray>() {
                return Ok(PythonValue::Blob(ba.to_vec()));
            }

            // Try to extract as bytes
            if let Ok(bytes) = obj.cast::<PyBytes>() {
                return Ok(PythonValue::Blob(bytes.as_bytes().to_vec()));
            }

            let b: Result<Blob, _> = obj.extract();
            if let Ok(b) = b {
                return Ok(PythonValue::Blob(b.v));
            }

            let l: PyResult<Vec<PythonValue>> = obj.extract();
            if let Ok(l) = l {
                return Ok(PythonValue::List(l));
            }

            let l: Result<List, _> = obj.extract();
            if let Ok(l) = l {
                return Ok(PythonValue::List(l.v));
            }

            let hm: PyResult<HashMap<PythonValue, PythonValue>> = obj.extract();
            if let Ok(hm) = hm {
                return Ok(PythonValue::HashMap(hm));
            }

            let geo: Result<GeoJSON, _> = obj.extract();
            if let Ok(geo) = geo {
                return Ok(PythonValue::GeoJSON(geo.v));
            }

            let hll: Result<HLL, _> = obj.extract();
            if let Ok(hll) = hll {
                return Ok(PythonValue::HLL(hll.v));
            }

            let vector: Result<Vector, _> = obj.extract();
            if let Ok(vector) = vector {
                return Ok(PythonValue::Vector(vector.v));
            }

            Err(PyTypeError::new_err("invalid value type"))
        }
    }

    impl From<HashMap<String, aerospike_core::Value>> for PythonValue {
        fn from(h: HashMap<String, aerospike_core::Value>) -> Self {
            let mut hash = HashMap::<PythonValue, PythonValue>::with_capacity(h.len());
            h.iter().for_each(|(k, v)| {
                hash.insert(PythonValue::String(k.into()), v.clone().into());
            });
            PythonValue::HashMap(hash)
        }
    }

    impl From<PythonValue> for aerospike_core::Value {
        fn from(other: PythonValue) -> Self {
            match other {
                PythonValue::Nil => aerospike_core::Value::Nil,
                PythonValue::Bool(b) => aerospike_core::Value::Bool(b),
                PythonValue::Int(i) => aerospike_core::Value::Int(i),
                PythonValue::Float(f) => aerospike_core::Value::Float(f64::from(f).into()),
                PythonValue::String(s) => aerospike_core::Value::String(s),
                PythonValue::Blob(b) => aerospike_core::Value::Blob(b),
                PythonValue::List(l) => {
                    let mut nl = Vec::<aerospike_core::Value>::with_capacity(l.len());
                    l.iter().for_each(|v| nl.push(v.clone().into()));
                    aerospike_core::Value::List(nl)
                }
                PythonValue::HashMap(h) => {
                    let mut arr = HashMap::with_capacity(h.len());
                    h.iter().for_each(|(k, v)| {
                        arr.insert(k.clone().into(), v.clone().into());
                    });
                    aerospike_core::Value::HashMap(arr)
                }
                PythonValue::OrderedMap(pairs) => {
                    // Insertion-ordered on the Python side maps to the core
                    // insertion-ordered map (IndexMap), preserving pair order
                    // on the wire.
                    let mut map = aerospike_core::IndexMap::with_capacity(pairs.len());
                    for (k, v) in pairs {
                        map.insert(k.into(), v.into());
                    }
                    aerospike_core::Value::OrderedMap(map)
                }
                PythonValue::GeoJSON(gj) => aerospike_core::Value::GeoJSON(gj),
                PythonValue::HLL(b) => aerospike_core::Value::HLL(b),
                PythonValue::Vector(v) => aerospike_core::Value::Vector(v),
                PythonValue::CdtSpecial(s) => match s {
                    SpecialValue::Null => aerospike_core::Value::Nil,
                    SpecialValue::Infinity => aerospike_core::Value::Infinity,
                    SpecialValue::Wildcard => aerospike_core::Value::Wildcard,
                },
            }
        }
    }

    impl From<aerospike_core::Value> for PythonValue {
        fn from(other: aerospike_core::Value) -> Self {
            match other {
                aerospike_core::Value::Nil => PythonValue::Nil,
                aerospike_core::Value::Bool(b) => PythonValue::Bool(b),
                aerospike_core::Value::Int(i) => PythonValue::Int(i),
                aerospike_core::Value::Float(fv) => {
                    PythonValue::Float(ordered_float::OrderedFloat(fv.into()))
                }
                aerospike_core::Value::String(s) => PythonValue::String(s),
                aerospike_core::Value::Blob(b) => PythonValue::Blob(b),
                aerospike_core::Value::List(l) => {
                    let mut nl = Vec::<PythonValue>::with_capacity(l.len());
                    l.iter().for_each(|v| nl.push(v.clone().into()));
                    PythonValue::List(nl)
                }
                aerospike_core::Value::MultiResult(mv) => {
                    // MultiResult is returned when server executes multiple operations for the same bin
                    // Convert to a list of PythonValues without flattening
                    let mut nl = Vec::<PythonValue>::with_capacity(mv.len());
                    mv.iter().for_each(|v| nl.push(v.clone().into()));
                    PythonValue::List(nl)
                }
                aerospike_core::Value::HashMap(h) => {
                    let mut arr = HashMap::with_capacity(h.len());
                    h.iter().for_each(|(k, v)| {
                        arr.insert(k.clone().into(), v.clone().into());
                    });
                    PythonValue::HashMap(arr)
                }
                aerospike_core::Value::OrderedMap(om) => {
                    let pairs: Vec<(PythonValue, PythonValue)> = om
                        .into_iter()
                        .map(|(k, v)| (k.into(), v.into()))
                        .collect();
                    PythonValue::OrderedMap(pairs)
                }
                aerospike_core::Value::SortedMap(sm) => {
                    // K-ordered server return: surface as an ordered map,
                    // preserving the server's canonical key order.
                    let pairs: Vec<(PythonValue, PythonValue)> = sm
                        .into_iter()
                        .map(|(k, v)| (k.into(), v.into()))
                        .collect();
                    PythonValue::OrderedMap(pairs)
                }
                aerospike_core::Value::Unknown(_code, bytes) => {
                    // Particle types this client does not interpret (legacy
                    // language-specific serializations, retired types). Surface
                    // the raw payload so the data is still accessible.
                    PythonValue::Blob(bytes)
                }
                aerospike_core::Value::GeoJSON(gj) => PythonValue::GeoJSON(gj),
                aerospike_core::Value::HLL(b) => PythonValue::HLL(b),
                aerospike_core::Value::Vector(v) => PythonValue::Vector(v),
                aerospike_core::Value::Infinity => PythonValue::CdtSpecial(SpecialValue::Infinity),
                aerospike_core::Value::Wildcard => PythonValue::CdtSpecial(SpecialValue::Wildcard),
                aerospike_core::Value::KeyValueList(kvl) => {
                    let pairs: Vec<(PythonValue, PythonValue)> = kvl
                        .into_iter()
                        .map(|(k, v)| (k.into(), v.into()))
                        .collect();
                    PythonValue::OrderedMap(pairs)
                }
            }
        }
    }

    impl PyStubType for PythonValue {
        fn type_output() -> TypeInfo {
            TypeInfo::any()
        }
    }

    // impl From<aerospike_core::Bin> for Bin {
    //     fn from(other: aerospike_core::Bin) -> Self {
    //         Bin { _as: other }
    //     }
    // }

    impl From<aerospike_core::Key> for Key {
        fn from(other: aerospike_core::Key) -> Self {
            Key { _as: other }
        }
    }

    impl From<aerospike_core::Record> for Record {
        fn from(other: aerospike_core::Record) -> Self {
            Record { _as: other, cached_bins: None, cached_results: None }
        }
    }

    #[cfg(test)]
    mod vector_python_value_tests {
        use super::*;

        // These exercise the PythonValue <-> aerospike_core::Value conversion
        // directly, without going through PyO3 (no GIL, no Python interpreter,
        // no server). The GIL-requiring half (constructing a Vector from a
        // Python list, its getters, etc.) is exercised separately once the
        // module is built and imported from Python.

        fn assert_round_trips(core_vector: aerospike_core::Vector) {
            let python_value = PythonValue::Vector(core_vector.clone());

            let value: aerospike_core::Value = python_value.clone().into();
            assert_eq!(value, aerospike_core::Value::Vector(core_vector.clone()));

            let back: PythonValue = value.into();
            assert_eq!(back, python_value);
        }

        #[test]
        fn float32_round_trips_through_value() {
            assert_round_trips(aerospike_core::Vector::float32(vec![0.12, 0.98, -0.34]));
        }

        #[test]
        fn float64_round_trips_through_value() {
            assert_round_trips(aerospike_core::Vector::float64(vec![1.5, -2.5, 3.25]));
        }

        #[test]
        fn int32_round_trips_through_value() {
            assert_round_trips(aerospike_core::Vector::int32(vec![-5, 0, 7, i32::MAX]));
        }

        #[test]
        fn float16_round_trips_through_value() {
            assert_round_trips(aerospike_core::Vector::float16(vec![0x3c00, 0x4000, 0xbc00]));
        }

        #[test]
        fn empty_vector_round_trips() {
            assert_round_trips(aerospike_core::Vector::float32(vec![]));
        }

        #[test]
        fn different_element_types_are_not_equal() {
            let a = PythonValue::Vector(aerospike_core::Vector::float32(vec![1.0]));
            let b = PythonValue::Vector(aerospike_core::Vector::float64(vec![1.0]));
            assert_ne!(a, b);
        }

        #[test]
        fn as_string_reports_vector() {
            let v = PythonValue::Vector(aerospike_core::Vector::int32(vec![1, 2, 3]));
            let s = v.as_string();
            assert!(s.contains("int32") || s.to_lowercase().contains("vector"), "got: {s}");
        }

        #[test]
        #[should_panic(expected = "cannot be used as map keys")]
        fn vector_cannot_be_hashed() {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::Hash;

            let v = PythonValue::Vector(aerospike_core::Vector::float32(vec![1.0]));
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
        }
    }
