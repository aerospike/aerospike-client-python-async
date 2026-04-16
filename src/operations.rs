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

use std::collections::HashMap;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};



use crate::cdt::*;
use crate::expressions::FilterExpression;
use crate::record::PythonValue;

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  OperationType (internal enum)
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Internal enum that owns all operation data, avoiding lifetime issues when converting to
    /// the core client's operation types.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum OperationType {
        /// Get operation - reads all bins from the record.
        Get(),
        /// Get operation - reads a specific bin from the record.
        GetBin(String),
        /// GetHeader operation - reads only record metadata (generation, TTL), no bin data.
        GetHeader(),
        /// Put operation - writes a bin to the record.
        Put(String, PythonValue),
        /// Add operation - increments/decrements an integer bin value.
        Add(String, PythonValue),
        /// Append operation - appends to a string bin value.
        Append(String, PythonValue),
        /// Prepend operation - prepends to a string bin value.
        Prepend(String, PythonValue),
        /// Delete operation - deletes the record.
        Delete(),
        /// Touch operation - updates the record's TTL without modifying bin data.
        Touch(),
        /// List get operation - gets element at index.
        ListGet(String, i64),
        /// List size operation - gets list size.
        ListSize(String),
        /// List pop operation - pops and returns element at index.
        ListPop(String, i64),
        /// List clear operation - clears the list.
        ListClear(String),
        /// List get_range operation - gets range of elements.
        ListGetRange(String, i64, i64),
        /// List set operation - sets element at index.
        ListSet(String, i64, PythonValue),
        /// List remove operation - removes element at index.
        ListRemove(String, i64),
        /// List remove_range operation - removes range of elements.
        ListRemoveRange(String, i64, i64),
        /// List get_range_from operation - gets range from index to end.
        ListGetRangeFrom(String, i64),
        /// List pop_range operation - pops range of elements.
        ListPopRange(String, i64, i64),
        /// List pop_range_from operation - pops range from index to end.
        ListPopRangeFrom(String, i64),
        /// List remove_range_from operation - removes range from index to end.
        ListRemoveRangeFrom(String, i64),
        /// List trim operation - trims list to range.
        ListTrim(String, i64, i64),
        /// List append operation - appends a value to the list (requires ListPolicy).
        ListAppend(String, PythonValue, ListPolicy),
        /// List append_items operation - appends multiple values to the list (requires ListPolicy).
        ListAppendItems(String, Vec<PythonValue>, ListPolicy),
        /// List insert operation - inserts a value at index (requires ListPolicy).
        ListInsert(String, i64, PythonValue, ListPolicy),
        /// List insert_items operation - inserts multiple values at index (requires ListPolicy).
        ListInsertItems(String, i64, Vec<PythonValue>, ListPolicy),
        /// List increment operation - increments element at index by value (requires ListPolicy).
        ListIncrement(String, i64, i64, ListPolicy),
        /// List sort operation - sorts the list (requires ListSortFlags).
        ListSort(String, ListSortFlags),
        /// List set_order operation - sets list order (ORDERED/UNORDERED).
        ListSetOrder(String, ListOrderType),
        /// List get_by_index operation - gets element by index with return type (requires ListReturnType).
        ListGetByIndex(String, i64, ListReturnType),
        /// List get_by_index_range operation - gets elements by index range with return type (requires ListReturnType).
        ListGetByIndexRange(String, i64, Option<i64>, ListReturnType),
        /// List get_by_rank operation - gets element by rank with return type (requires ListReturnType).
        ListGetByRank(String, i64, ListReturnType),
        /// List get_by_rank_range operation - gets elements by rank range with return type (requires ListReturnType).
        ListGetByRankRange(String, i64, Option<i64>, ListReturnType),
        /// List get_by_value operation - gets elements by value with return type (requires ListReturnType).
        ListGetByValue(String, PythonValue, ListReturnType),
        /// List get_by_value_range operation - gets elements by value range with return type (requires ListReturnType).
        ListGetByValueRange(String, PythonValue, PythonValue, ListReturnType),
        /// List get_by_value_list operation - gets elements by value list with return type (requires ListReturnType).
        ListGetByValueList(String, Vec<PythonValue>, ListReturnType),
        /// List get_by_value_relative_rank_range operation - gets elements by value relative rank range (requires ListReturnType).
        ListGetByValueRelativeRankRange(String, PythonValue, i64, Option<i64>, ListReturnType),
        /// List remove_by_index operation - removes element by index with return type (requires ListReturnType).
        ListRemoveByIndex(String, i64, ListReturnType),
        /// List remove_by_index_range operation - removes elements by index range with return type (requires ListReturnType).
        ListRemoveByIndexRange(String, i64, Option<i64>, ListReturnType),
        /// List remove_by_rank operation - removes element by rank with return type (requires ListReturnType).
        ListRemoveByRank(String, i64, ListReturnType),
        /// List remove_by_rank_range operation - removes elements by rank range with return type (requires ListReturnType).
        ListRemoveByRankRange(String, i64, Option<i64>, ListReturnType),
        /// List remove_by_value operation - removes elements by value with return type (requires ListReturnType).
        ListRemoveByValue(String, PythonValue, ListReturnType),
        /// List remove_by_value_list operation - removes elements by value list with return type (requires ListReturnType).
        ListRemoveByValueList(String, Vec<PythonValue>, ListReturnType),
        /// List remove_by_value_range operation - removes elements by value range with return type (requires ListReturnType).
        ListRemoveByValueRange(String, PythonValue, PythonValue, ListReturnType),
        /// List remove_by_value_relative_rank_range operation - removes elements by value relative rank range (requires ListReturnType).
        ListRemoveByValueRelativeRankRange(String, PythonValue, i64, Option<i64>, ListReturnType),
        /// List create operation - creates a list with order and persisted index.
        ListCreate(String, ListOrderType, bool, bool),
        /// List set_with_policy operation - sets element at index with list policy.
        ListSetWithPolicy(String, i64, PythonValue, ListPolicy),
        /// List increment_by_one operation - increments element at index by 1.
        ListIncrementByOne(String, i64),
        /// List increment_by_one_with_policy operation - increments element at index by 1 with policy.
        ListIncrementByOneWithPolicy(String, i64, ListPolicy),
        /// List create_with_index operation - creates list with persisted index.
        ListCreateWithIndex(String, ListOrderType),
        /// List set_order_with_index operation - sets list order with persisted index.
        ListSetOrderWithIndex(String, ListOrderType),
        /// Map size operation - gets map size.
        MapSize(String),
        /// Map clear operation - clears the map.
        MapClear(String),
        /// Map put operation - puts a key-value pair (requires MapPolicy).
        MapPut(String, PythonValue, PythonValue, MapPolicy),
        /// Map put_items operation - puts multiple key-value pairs (requires MapPolicy).
        MapPutItems(String, Vec<(PythonValue, PythonValue)>, MapPolicy),
        /// Map increment_value operation - increments value by key (requires MapPolicy).
        MapIncrementValue(String, PythonValue, i64, MapPolicy),
        /// Map decrement_value operation - decrements value by key (requires MapPolicy).
        MapDecrementValue(String, PythonValue, i64, MapPolicy),
        /// Map get_by_key operation - gets value by key (requires MapReturnType).
        MapGetByKey(String, PythonValue, MapReturnType),
        /// Map remove_by_key operation - removes item by key (requires MapReturnType).
        MapRemoveByKey(String, PythonValue, MapReturnType),
        /// Map get_by_key_range operation - gets items by key range (requires MapReturnType).
        MapGetByKeyRange(String, PythonValue, PythonValue, MapReturnType),
        /// Map remove_by_key_range operation - removes items by key range (requires MapReturnType).
        MapRemoveByKeyRange(String, PythonValue, PythonValue, MapReturnType),
        /// Map get_by_index operation - gets item by index (requires MapReturnType).
        MapGetByIndex(String, i64, MapReturnType),
        /// Map remove_by_index operation - removes item by index (requires MapReturnType).
        MapRemoveByIndex(String, i64, MapReturnType),
        /// Map get_by_index_range operation - gets items by index range (requires MapReturnType).
        MapGetByIndexRange(String, i64, i64, MapReturnType),
        /// Map remove_by_index_range operation - removes items by index range (requires MapReturnType).
        MapRemoveByIndexRange(String, i64, i64, MapReturnType),
        /// Map get_by_index_range_from operation - gets items from index to end (requires MapReturnType).
        MapGetByIndexRangeFrom(String, i64, MapReturnType),
        /// Map remove_by_index_range_from operation - removes items from index to end (requires MapReturnType).
        MapRemoveByIndexRangeFrom(String, i64, MapReturnType),
        /// Map get_by_rank operation - gets item by rank (requires MapReturnType).
        MapGetByRank(String, i64, MapReturnType),
        /// Map remove_by_rank operation - removes item by rank (requires MapReturnType).
        MapRemoveByRank(String, i64, MapReturnType),
        /// Map get_by_rank_range operation - gets items by rank range (requires MapReturnType).
        MapGetByRankRange(String, i64, i64, MapReturnType),
        /// Map remove_by_rank_range operation - removes items by rank range (requires MapReturnType).
        MapRemoveByRankRange(String, i64, i64, MapReturnType),
        /// Map get_by_rank_range_from operation - gets items from rank to end (requires MapReturnType).
        MapGetByRankRangeFrom(String, i64, MapReturnType),
        /// Map remove_by_rank_range_from operation - removes items from rank to end (requires MapReturnType).
        MapRemoveByRankRangeFrom(String, i64, MapReturnType),
        /// Map get_by_value operation - gets items by value (requires MapReturnType).
        MapGetByValue(String, PythonValue, MapReturnType),
        /// Map remove_by_value operation - removes items by value (requires MapReturnType).
        MapRemoveByValue(String, PythonValue, MapReturnType),
        /// Map get_by_value_range operation - gets items by value range (requires MapReturnType).
        MapGetByValueRange(String, PythonValue, PythonValue, MapReturnType),
        /// Map remove_by_value_range operation - removes items by value range (requires MapReturnType).
        MapRemoveByValueRange(String, PythonValue, PythonValue, MapReturnType),
        /// Map get_by_key_list operation - gets items by a list of keys (requires MapReturnType).
        MapGetByKeyList(String, Vec<PythonValue>, MapReturnType),
        /// Map remove_by_key_list operation - removes items by a list of keys (requires MapReturnType).
        MapRemoveByKeyList(String, Vec<PythonValue>, MapReturnType),
        /// Map get_by_value_list operation - gets items by a list of values (requires MapReturnType).
        MapGetByValueList(String, Vec<PythonValue>, MapReturnType),
        /// Map remove_by_value_list operation - removes items by a list of values (requires MapReturnType).
        MapRemoveByValueList(String, Vec<PythonValue>, MapReturnType),
        /// Map set_map_policy operation - sets map policy.
        MapSetMapPolicy(String, MapPolicy),
        /// Map get_by_key_relative_index_range operation - gets items by key relative index range (requires MapReturnType).
        MapGetByKeyRelativeIndexRange(String, PythonValue, i64, Option<i64>, MapReturnType),
        /// Map get_by_value_relative_rank_range operation - gets items by value relative rank range (requires MapReturnType).
        MapGetByValueRelativeRankRange(String, PythonValue, i64, Option<i64>, MapReturnType),
        /// Map remove_by_key_relative_index_range operation - removes items by key relative index range (requires MapReturnType).
        MapRemoveByKeyRelativeIndexRange(String, PythonValue, i64, Option<i64>, MapReturnType),
        /// Map remove_by_value_relative_rank_range operation - removes items by value relative rank range (requires MapReturnType).
        MapRemoveByValueRelativeRankRange(String, PythonValue, i64, Option<i64>, MapReturnType),
        /// Map create operation - creates a map with order.
        MapCreate(String, MapOrder),
        /// Map create_with_index operation - creates map with persisted index.
        MapCreateWithIndex(String, MapOrder),
        /// Map set_policy operation - sets map policy (full policy including persist_index).
        MapSetPolicy(String, MapPolicy),
        /// Bit resize operation - resizes byte array (requires BitPolicy).
        BitResize(String, i64, Option<BitwiseResizeFlags>, BitPolicy),
        /// Bit insert operation - inserts bytes (requires BitPolicy).
        BitInsert(String, i64, PythonValue, BitPolicy),
        /// Bit remove operation - removes bytes (requires BitPolicy).
        BitRemove(String, i64, i64, BitPolicy),
        /// Bit set operation - sets bits (requires BitPolicy).
        BitSet(String, i64, i64, PythonValue, BitPolicy),
        /// Bit or operation - performs bitwise OR (requires BitPolicy).
        BitOr(String, i64, i64, PythonValue, BitPolicy),
        /// Bit xor operation - performs bitwise XOR (requires BitPolicy).
        BitXor(String, i64, i64, PythonValue, BitPolicy),
        /// Bit and operation - performs bitwise AND (requires BitPolicy).
        BitAnd(String, i64, i64, PythonValue, BitPolicy),
        /// Bit not operation - performs bitwise NOT (requires BitPolicy).
        BitNot(String, i64, i64, BitPolicy),
        /// Bit lshift operation - performs left shift (requires BitPolicy).
        BitLShift(String, i64, i64, i64, BitPolicy),
        /// Bit rshift operation - performs right shift (requires BitPolicy).
        BitRShift(String, i64, i64, i64, BitPolicy),
        /// Bit add operation - adds to integer value (requires BitPolicy).
        BitAdd(String, i64, i64, i64, bool, BitwiseOverflowActions, BitPolicy),
        /// Bit subtract operation - subtracts from integer value (requires BitPolicy).
        BitSubtract(String, i64, i64, i64, bool, BitwiseOverflowActions, BitPolicy),
        /// Bit set_int operation - sets integer value (requires BitPolicy).
        BitSetInt(String, i64, i64, i64, BitPolicy),
        /// Bit get operation - gets bits (read-only).
        BitGet(String, i64, i64),
        /// Bit count operation - counts set bits (read-only).
        BitCount(String, i64, i64),
        /// Bit lscan operation - scans left for value (read-only).
        BitLScan(String, i64, i64, bool),
        /// Bit rscan operation - scans right for value (read-only).
        BitRScan(String, i64, i64, bool),
        /// Bit get_int operation - gets integer value (read-only).
        BitGetInt(String, i64, i64, bool),
        /// HLL init operation - creates or resets an HLL bin.
        HllInit(String, i64, i64, i64),
        /// HLL add operation - adds values to HLL.
        HllAdd(String, Vec<PythonValue>, i64, i64, i64),
        /// HLL get_count operation - returns estimated count.
        HllGetCount(String),
        /// HLL describe operation - returns index_bit_count and min_hash_bit_count.
        HllDescribe(String),
        /// HLL refresh_count operation - updates cached count.
        HllRefreshCount(String),
        /// HLL fold operation - folds HLL to specified index_bit_count.
        HllFold(String, i64),
        /// HLL get_union operation - returns union of HLL objects.
        HllGetUnion(String, Vec<PythonValue>),
        /// HLL get_union_count operation - returns estimated union count.
        HllGetUnionCount(String, Vec<PythonValue>),
        /// HLL get_intersect_count operation - returns estimated intersection count.
        HllGetIntersectCount(String, Vec<PythonValue>),
        /// HLL get_similarity operation - returns estimated similarity.
        HllGetSimilarity(String, Vec<PythonValue>),
        /// HLL set_union operation - sets union of HLL objects.
        HllSetUnion(String, Vec<PythonValue>, i64),
        /// Expression read operation - evaluates expression and returns result.
        ExpRead(String, FilterExpression, i64),
        /// Expression write operation - evaluates expression and writes result to bin.
        ExpWrite(String, FilterExpression, i64),
    }

    /// Python wrapper for Operation enum.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct Operation {
        pub(crate) op: OperationType,
    }

    impl Default for Operation {
        fn default() -> Self {
            Operation {
                op: OperationType::Get(),
            }
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Operation {
        #[new]
        pub fn new() -> Self {
            Self::default()
        }

        /// Create a Get operation (reads all bins).
        #[staticmethod]
        pub fn get() -> Self {
            Operation {
                op: OperationType::Get(),
            }
        }

        /// Create a Get operation for a specific bin.
        #[staticmethod]
        pub fn get_bin(bin_name: String) -> Self {
            Operation {
                op: OperationType::GetBin(bin_name),
            }
        }

        /// Create a Put operation.
        #[staticmethod]
        pub fn put(bin_name: String, value: PythonValue) -> Self {
            Operation {
                op: OperationType::Put(bin_name, value),
            }
        }

        /// Create a GetHeader operation (metadata only, no bin data).
        #[staticmethod]
        pub fn get_header() -> Self {
            Operation {
                op: OperationType::GetHeader(),
            }
        }

        /// Create a Delete operation.
        #[staticmethod]
        pub fn delete() -> Self {
            Operation {
                op: OperationType::Delete(),
            }
        }

        /// Create a Touch operation (updates TTL).
        #[staticmethod]
        pub fn touch() -> Self {
            Operation {
                op: OperationType::Touch(),
            }
        }

        /// Create an Add operation (increments/decrements integer bin value).
        #[staticmethod]
        pub fn add(bin_name: String, value: PythonValue) -> Self {
            Operation {
                op: OperationType::Add(bin_name, value),
            }
        }

        /// Create an Append operation (appends to string bin value).
        #[staticmethod]
        pub fn append(bin_name: String, value: PythonValue) -> Self {
            Operation {
                op: OperationType::Append(bin_name, value),
            }
        }

        /// Create a Prepend operation (prepends to string bin value).
        #[staticmethod]
        pub fn prepend(bin_name: String, value: PythonValue) -> Self {
            Operation {
                op: OperationType::Prepend(bin_name, value),
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ListOperation
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// List bin operations. Create list operations used by the client's `operate()` method.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct ListOperation {
        pub(crate) op: OperationType,
        pub(crate) ctx: Option<Vec<CTX>>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl ListOperation {
        /// Create a List get operation (gets element at index).
        #[staticmethod]
        pub fn get(bin_name: String, index: i64) -> Self {
            ListOperation {
                op: OperationType::ListGet(bin_name, index),
                ctx: None,
            }
        }

        /// Create a List size operation (gets list size).
        #[staticmethod]
        pub fn size(bin_name: String) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListSize(bin_name),
            }
        }

        /// Create a List pop operation (pops and returns element at index).
        #[staticmethod]
        pub fn pop(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListPop(bin_name, index),
            }
        }

        /// Create a List clear operation (clears the list).
        #[staticmethod]
        pub fn clear(bin_name: String) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListClear(bin_name),
            }
        }

        /// Create a List get_range operation (gets range of elements).
        #[staticmethod]
        pub fn get_range(bin_name: String, index: i64, count: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetRange(bin_name, index, count),
            }
        }

        /// Create a List set operation (sets element at index).
        #[staticmethod]
        pub fn set(bin_name: String, index: i64, value: PythonValue) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListSet(bin_name, index, value),
            }
        }

        /// Create a List set_with_policy operation (sets element at index with list policy).
        #[staticmethod]
        pub fn set_with_policy(bin_name: String, policy: ListPolicy, index: i64, value: PythonValue) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListSetWithPolicy(bin_name, index, value, policy),
            }
        }

        /// Create a List remove operation (removes element at index).
        #[staticmethod]
        pub fn remove(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemove(bin_name, index),
            }
        }

        /// Create a List remove_range operation (removes range of elements).
        #[staticmethod]
        pub fn remove_range(bin_name: String, index: i64, count: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveRange(bin_name, index, count),
            }
        }

        /// Create a List get_range_from operation (gets range from index to end).
        #[staticmethod]
        pub fn get_range_from(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetRangeFrom(bin_name, index),
            }
        }

        /// Create a List pop_range operation (pops range of elements).
        #[staticmethod]
        pub fn pop_range(bin_name: String, index: i64, count: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListPopRange(bin_name, index, count),
            }
        }

        /// Create a List pop_range_from operation (pops range from index to end).
        #[staticmethod]
        pub fn pop_range_from(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListPopRangeFrom(bin_name, index),
            }
        }

        /// Create a List remove_range_from operation (removes range from index to end).
        #[staticmethod]
        pub fn remove_range_from(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveRangeFrom(bin_name, index),
            }
        }

        /// Create a List trim operation (trims list to range).
        #[staticmethod]
        pub fn trim(bin_name: String, index: i64, count: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListTrim(bin_name, index, count),
            }
        }

        /// Create a List append operation (appends a value to the list, requires ListPolicy).
        #[staticmethod]
        pub fn append(bin_name: String, value: PythonValue, policy: ListPolicy) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListAppend(bin_name, value, policy),
            }
        }

        /// Create a List append_items operation (appends multiple values to the list, requires ListPolicy).
        #[staticmethod]
        pub fn append_items(bin_name: String, values: Vec<PythonValue>, policy: ListPolicy) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListAppendItems(bin_name, values, policy),
            }
        }

        /// Create a List insert operation (inserts a value at index, requires ListPolicy).
        #[staticmethod]
        pub fn insert(bin_name: String, index: i64, value: PythonValue, policy: ListPolicy) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListInsert(bin_name, index, value, policy),
            }
        }

        /// Create a List insert_items operation (inserts multiple values at index, requires ListPolicy).
        #[staticmethod]
        pub fn insert_items(bin_name: String, index: i64, values: Vec<PythonValue>, policy: ListPolicy) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListInsertItems(bin_name, index, values, policy),
            }
        }

        /// Create a List increment operation (increments element at index by value, requires ListPolicy).
        #[staticmethod]
        pub fn increment(bin_name: String, index: i64, value: i64, policy: ListPolicy) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListIncrement(bin_name, index, value, policy),
            }
        }

        /// Create a List increment_by_one operation (increments element at index by 1).
        #[staticmethod]
        pub fn increment_by_one(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListIncrementByOne(bin_name, index),
            }
        }

        /// Create a List increment_by_one_with_policy operation (increments element at index by 1 with policy).
        #[staticmethod]
        pub fn increment_by_one_with_policy(bin_name: String, policy: ListPolicy, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListIncrementByOneWithPolicy(bin_name, index, policy),
            }
        }

        /// Create a List sort operation (sorts the list, requires ListSortFlags).
        #[staticmethod]
        pub fn sort(bin_name: String, flags: ListSortFlags) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListSort(bin_name, flags),
            }
        }

        /// Create a List set_order operation (sets list order ORDERED/UNORDERED).
        #[staticmethod]
        pub fn set_order(bin_name: String, order: ListOrderType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListSetOrder(bin_name, order),
            }
        }

        /// Create a List get_by_index operation (gets element by index with return type, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_index(bin_name: String, index: i64, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByIndex(bin_name, index, return_type),
            }
        }

        /// Create a List get_by_index_range operation (gets elements by index range with return type, requires ListReturnType).
        /// If count is None, gets from index to end of list.
        #[staticmethod]
        pub fn get_by_index_range(bin_name: String, index: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByIndexRange(bin_name, index, count, return_type),
            }
        }

        /// Create a List get_by_rank operation (gets element by rank with return type, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_rank(bin_name: String, rank: i64, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByRank(bin_name, rank, return_type),
            }
        }

        /// Create a List get_by_rank_range operation (gets elements by rank range with return type, requires ListReturnType).
        /// If count is None, gets from rank to end of list.
        #[staticmethod]
        pub fn get_by_rank_range(bin_name: String, rank: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByRankRange(bin_name, rank, count, return_type),
            }
        }

        /// Create a List get_by_value operation (gets elements by value with return type, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_value(bin_name: String, value: PythonValue, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByValue(bin_name, value, return_type),
            }
        }

        /// Create a List get_by_value_range operation (gets elements by value range with return type, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_value_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByValueRange(bin_name, begin, end, return_type),
            }
        }

        /// Create a List get_by_value_list operation (gets elements by value list with return type, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_value_list(bin_name: String, values: Vec<PythonValue>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByValueList(bin_name, values, return_type),
            }
        }

        /// Create a List get_by_value_relative_rank_range operation (gets elements by value relative rank range, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_value_relative_rank_range(bin_name: String, value: PythonValue, rank: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByValueRelativeRankRange(bin_name, value, rank, count, return_type),
            }
        }

        /// Create a List remove_by_index operation (removes element by index with return type, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_index(bin_name: String, index: i64, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByIndex(bin_name, index, return_type),
            }
        }

        /// Create a List remove_by_index_range operation (removes elements by index range with return type, requires ListReturnType).
        /// If count is None, removes from index to end of list.
        #[staticmethod]
        pub fn remove_by_index_range(bin_name: String, index: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByIndexRange(bin_name, index, count, return_type),
            }
        }

        /// Create a List remove_by_rank operation (removes element by rank with return type, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_rank(bin_name: String, rank: i64, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByRank(bin_name, rank, return_type),
            }
        }

        /// Create a List remove_by_rank_range operation (removes elements by rank range with return type, requires ListReturnType).
        /// If count is None, removes from rank to end of list.
        #[staticmethod]
        pub fn remove_by_rank_range(bin_name: String, rank: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByRankRange(bin_name, rank, count, return_type),
            }
        }

        /// Create a List remove_by_value operation (removes elements by value with return type, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_value(bin_name: String, value: PythonValue, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByValue(bin_name, value, return_type),
            }
        }

        /// Create a List remove_by_value_list operation (removes elements by value list with return type, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_value_list(bin_name: String, values: Vec<PythonValue>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByValueList(bin_name, values, return_type),
            }
        }

        /// Create a List remove_by_value_range operation (removes elements by value range with return type, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_value_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByValueRange(bin_name, begin, end, return_type),
            }
        }

        /// Create a List remove_by_value_relative_rank_range operation (removes elements by value relative rank range, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_value_relative_rank_range(bin_name: String, value: PythonValue, rank: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByValueRelativeRankRange(bin_name, value, rank, count, return_type),
            }
        }

        /// Create a List create operation (creates a list with order and persisted index).
        #[staticmethod]
        pub fn create(bin_name: String, order: ListOrderType, pad: bool, persist_index: bool) -> Self {
            ListOperation {
                op: OperationType::ListCreate(bin_name, order, pad, persist_index),
                ctx: None,
            }
        }

        /// Create a List create_with_index operation (creates list with persisted index).
        #[staticmethod]
        pub fn create_with_index(bin_name: String, order: ListOrderType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListCreateWithIndex(bin_name, order),
            }
        }

        /// Create a List set_order_with_index operation (sets list order with persisted index).
        #[staticmethod]
        pub fn set_order_with_index(bin_name: String, order: ListOrderType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListSetOrderWithIndex(bin_name, order),
            }
        }

        /// Set the context for this operation. Used for nested CDT operations.
        pub fn set_context(&self, ctx: Vec<CTX>) -> Self {
            ListOperation {
                op: self.op.clone(),
                ctx: Some(ctx),
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  MapOperation
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Map bin operations. Create map operations used by the client's `operate()` method.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct MapOperation {
        pub(crate) op: OperationType,
        pub(crate) ctx: Option<Vec<CTX>>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl MapOperation {
        /// Create a Map size operation (gets map size).
        #[staticmethod]
        pub fn size(bin_name: String) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapSize(bin_name),
            }
        }

        #[staticmethod]
        pub fn clear(bin_name: String) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapClear(bin_name),
            }
        }

        #[staticmethod]
        pub fn put(bin_name: String, key: PythonValue, value: PythonValue, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapPut(bin_name, key, value, policy),
            }
        }

        #[staticmethod]
        pub fn put_items(bin_name: String, items: Vec<(PythonValue, PythonValue)>, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapPutItems(bin_name, items, policy),
            }
        }

        #[staticmethod]
        pub fn increment_value(bin_name: String, key: PythonValue, value: i64, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapIncrementValue(bin_name, key, value, policy),
            }
        }

        #[staticmethod]
        pub fn decrement_value(bin_name: String, key: PythonValue, value: i64, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapDecrementValue(bin_name, key, value, policy),
            }
        }

        #[staticmethod]
        pub fn get_by_key(bin_name: String, key: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByKey(bin_name, key, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_key(bin_name: String, key: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByKey(bin_name, key, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_key_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByKeyRange(bin_name, begin, end, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_key_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByKeyRange(bin_name, begin, end, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_index(bin_name: String, index: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByIndex(bin_name, index, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_index(bin_name: String, index: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByIndex(bin_name, index, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_index_range(bin_name: String, index: i64, count: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByIndexRange(bin_name, index, count, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_index_range(bin_name: String, index: i64, count: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByIndexRange(bin_name, index, count, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_index_range_from(bin_name: String, index: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByIndexRangeFrom(bin_name, index, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_index_range_from(bin_name: String, index: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByIndexRangeFrom(bin_name, index, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_rank(bin_name: String, rank: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByRank(bin_name, rank, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_rank(bin_name: String, rank: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByRank(bin_name, rank, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_rank_range(bin_name: String, rank: i64, count: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByRankRange(bin_name, rank, count, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_rank_range(bin_name: String, rank: i64, count: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByRankRange(bin_name, rank, count, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_rank_range_from(bin_name: String, rank: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByRankRangeFrom(bin_name, rank, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_rank_range_from(bin_name: String, rank: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByRankRangeFrom(bin_name, rank, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_value(bin_name: String, value: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByValue(bin_name, value, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_value(bin_name: String, value: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByValue(bin_name, value, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_value_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByValueRange(bin_name, begin, end, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_value_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByValueRange(bin_name, begin, end, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_key_list(bin_name: String, keys: Vec<PythonValue>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByKeyList(bin_name, keys, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_key_list(bin_name: String, keys: Vec<PythonValue>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByKeyList(bin_name, keys, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_value_list(bin_name: String, values: Vec<PythonValue>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByValueList(bin_name, values, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_value_list(bin_name: String, values: Vec<PythonValue>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByValueList(bin_name, values, return_type),
            }
        }

        /// Create a Map set_map_policy operation (sets map policy).
        #[staticmethod]
        pub fn set_map_policy(bin_name: String, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapSetMapPolicy(bin_name, policy),
            }
        }

        /// Create a Map get_by_key_relative_index_range operation (gets items by key relative index range, requires MapReturnType).
        #[staticmethod]
        pub fn get_by_key_relative_index_range(bin_name: String, key: PythonValue, index: i64, count: Option<i64>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByKeyRelativeIndexRange(bin_name, key, index, count, return_type),
            }
        }

        /// Create a Map get_by_value_relative_rank_range operation (gets items by value relative rank range, requires MapReturnType).
        #[staticmethod]
        pub fn get_by_value_relative_rank_range(bin_name: String, value: PythonValue, rank: i64, count: Option<i64>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByValueRelativeRankRange(bin_name, value, rank, count, return_type),
            }
        }

        /// Create a Map remove_by_key_relative_index_range operation (removes items by key relative index range, requires MapReturnType).
        #[staticmethod]
        pub fn remove_by_key_relative_index_range(bin_name: String, key: PythonValue, index: i64, count: Option<i64>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByKeyRelativeIndexRange(bin_name, key, index, count, return_type),
            }
        }

        /// Create a Map remove_by_value_relative_rank_range operation (removes items by value relative rank range, requires MapReturnType).
        #[staticmethod]
        pub fn remove_by_value_relative_rank_range(bin_name: String, value: PythonValue, rank: i64, count: Option<i64>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByValueRelativeRankRange(bin_name, value, rank, count, return_type),
            }
        }

        /// Create a Map create operation (creates a map with order).
        #[staticmethod]
        pub fn create(bin_name: String, order: MapOrder) -> Self {
            MapOperation {
                op: OperationType::MapCreate(bin_name, order),
                ctx: None,
            }
        }

        /// Create a Map create_with_index operation (creates map with persisted index).
        #[staticmethod]
        pub fn create_with_index(bin_name: String, order: MapOrder) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapCreateWithIndex(bin_name, order),
            }
        }

        /// Create a Map set_policy operation (sets full map policy including order and persist_index).
        #[staticmethod]
        pub fn set_policy(bin_name: String, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapSetPolicy(bin_name, policy),
            }
        }

        /// Set the context for this operation. Used for nested CDT operations.
        pub fn set_context(&self, ctx: Vec<CTX>) -> Self {
            MapOperation {
                op: self.op.clone(),
                ctx: Some(ctx),
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BitOperation
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Bit operations. Create bit operations used by the client's `operate()` method.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct BitOperation {
        pub(crate) op: OperationType,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BitOperation {
        /// Create a Bit resize operation (resizes byte array, requires BitPolicy).
        #[staticmethod]
        pub fn resize(bin_name: String, byte_size: i64, resize_flags: Option<BitwiseResizeFlags>, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitResize(bin_name, byte_size, resize_flags, policy),
            }
        }
        /// Create a Bit insert operation (inserts bytes, requires BitPolicy).
        #[staticmethod]
        pub fn insert(bin_name: String, byte_offset: i64, value: PythonValue, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitInsert(bin_name, byte_offset, value, policy),
            }
        }

        /// Create a Bit remove operation (removes bytes, requires BitPolicy).
        #[staticmethod]
        pub fn remove(bin_name: String, byte_offset: i64, byte_size: i64, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitRemove(bin_name, byte_offset, byte_size, policy),
            }
        }

        /// Create a Bit set operation (sets bits, requires BitPolicy).
        #[staticmethod]
        pub fn set(bin_name: String, bit_offset: i64, bit_size: i64, value: PythonValue, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitSet(bin_name, bit_offset, bit_size, value, policy),
            }
        }

        /// Create a Bit or operation (performs bitwise OR, requires BitPolicy).
        #[staticmethod]
        pub fn or(bin_name: String, bit_offset: i64, bit_size: i64, value: PythonValue, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitOr(bin_name, bit_offset, bit_size, value, policy),
            }
        }

        /// Create a Bit xor operation (performs bitwise XOR, requires BitPolicy).
        #[staticmethod]
        pub fn xor(bin_name: String, bit_offset: i64, bit_size: i64, value: PythonValue, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitXor(bin_name, bit_offset, bit_size, value, policy),
            }
        }

        /// Create a Bit and operation (performs bitwise AND, requires BitPolicy).
        #[staticmethod]
        pub fn and(bin_name: String, bit_offset: i64, bit_size: i64, value: PythonValue, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitAnd(bin_name, bit_offset, bit_size, value, policy),
            }
        }

        /// Create a Bit not operation (performs bitwise NOT, requires BitPolicy).
        #[staticmethod]
        pub fn not(bin_name: String, bit_offset: i64, bit_size: i64, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitNot(bin_name, bit_offset, bit_size, policy),
            }
        }

        /// Create a Bit lshift operation (performs left shift, requires BitPolicy).
        #[staticmethod]
        pub fn lshift(bin_name: String, bit_offset: i64, bit_size: i64, shift: i64, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitLShift(bin_name, bit_offset, bit_size, shift, policy),
            }
        }

        /// Create a Bit rshift operation (performs right shift, requires BitPolicy).
        #[staticmethod]
        pub fn rshift(bin_name: String, bit_offset: i64, bit_size: i64, shift: i64, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitRShift(bin_name, bit_offset, bit_size, shift, policy),
            }
        }

        /// Create a Bit add operation (adds to integer value, requires BitPolicy).
        #[staticmethod]
        pub fn add(bin_name: String, bit_offset: i64, bit_size: i64, value: i64, signed: bool, action: BitwiseOverflowActions, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitAdd(bin_name, bit_offset, bit_size, value, signed, action, policy),
            }
        }

        /// Create a Bit subtract operation (subtracts from integer value, requires BitPolicy).
        #[staticmethod]
        pub fn subtract(bin_name: String, bit_offset: i64, bit_size: i64, value: i64, signed: bool, action: BitwiseOverflowActions, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitSubtract(bin_name, bit_offset, bit_size, value, signed, action, policy),
            }
        }

        /// Create a Bit set_int operation (sets integer value, requires BitPolicy).
        #[staticmethod]
        pub fn set_int(bin_name: String, bit_offset: i64, bit_size: i64, value: i64, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitSetInt(bin_name, bit_offset, bit_size, value, policy),
            }
        }

        /// Create a Bit get operation (gets bits, read-only).
        #[staticmethod]
        pub fn get(bin_name: String, bit_offset: i64, bit_size: i64) -> Self {
            BitOperation {
                op: OperationType::BitGet(bin_name, bit_offset, bit_size),
            }
        }

        /// Create a Bit count operation (counts set bits, read-only).
        #[staticmethod]
        pub fn count(bin_name: String, bit_offset: i64, bit_size: i64) -> Self {
            BitOperation {
                op: OperationType::BitCount(bin_name, bit_offset, bit_size),
            }
        }

        /// Create a Bit lscan operation (scans left for value, read-only).
        #[staticmethod]
        pub fn lscan(bin_name: String, bit_offset: i64, bit_size: i64, value: bool) -> Self {
            BitOperation {
                op: OperationType::BitLScan(bin_name, bit_offset, bit_size, value),
            }
        }

        /// Create a Bit rscan operation (scans right for value, read-only).
        #[staticmethod]
        pub fn rscan(bin_name: String, bit_offset: i64, bit_size: i64, value: bool) -> Self {
            BitOperation {
                op: OperationType::BitRScan(bin_name, bit_offset, bit_size, value),
            }
        }

        /// Create a Bit get_int operation (gets integer value, read-only).
        #[staticmethod]
        pub fn get_int(bin_name: String, bit_offset: i64, bit_size: i64, signed: bool) -> Self {
            BitOperation {
                op: OperationType::BitGetInt(bin_name, bit_offset, bit_size, signed),
            }
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  HllOperation
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// HLL (HyperLogLog) operations. Create HLL operations used by the client's `operate()` method.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct HllOperation {
        pub(crate) op: OperationType,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl HllOperation {
        /// Create HLL init operation.
        /// Server creates a new HLL or resets an existing HLL.
        /// Server does not return a value.
        #[staticmethod]
        #[pyo3(signature = (bin_name, index_bit_count, min_hash_bit_count = -1, flags = 0))]
        pub fn init(bin_name: String, index_bit_count: i64, min_hash_bit_count: i64, flags: i64) -> Self {
            HllOperation {
                op: OperationType::HllInit(bin_name, index_bit_count, min_hash_bit_count, flags),
            }
        }

        /// Create HLL add operation.
        /// Server adds values to HLL set. If HLL bin does not exist and index_bit_count is set,
        /// a new HLL bin will be created.
        /// Server returns number of entries that caused HLL to update a register.
        #[staticmethod]
        #[pyo3(signature = (bin_name, values, index_bit_count = -1, min_hash_bit_count = -1, flags = 0))]
        pub fn add(bin_name: String, values: Vec<PythonValue>, index_bit_count: i64, min_hash_bit_count: i64, flags: i64) -> Self {
            HllOperation {
                op: OperationType::HllAdd(bin_name, values, index_bit_count, min_hash_bit_count, flags),
            }
        }

        /// Create HLL get_count operation.
        /// Server returns estimated number of elements in the HLL bin.
        #[staticmethod]
        pub fn get_count(bin_name: String) -> Self {
            HllOperation {
                op: OperationType::HllGetCount(bin_name),
            }
        }

        /// Create HLL describe operation.
        /// Server returns index_bit_count and min_hash_bit_count used to create HLL bin
        /// in a list of longs. The list size is 2.
        #[staticmethod]
        pub fn describe(bin_name: String) -> Self {
            HllOperation {
                op: OperationType::HllDescribe(bin_name),
            }
        }

        /// Create HLL refresh_count operation.
        /// Server updates the cached count (if stale) and returns the count.
        #[staticmethod]
        pub fn refresh_count(bin_name: String) -> Self {
            HllOperation {
                op: OperationType::HllRefreshCount(bin_name),
            }
        }

        /// Create HLL fold operation.
        /// Server folds index_bit_count to the specified value.
        /// This can only be applied when min_hash_bit_count on the HLL bin is 0.
        /// Server does not return a value.
        #[staticmethod]
        pub fn fold(bin_name: String, index_bit_count: i64) -> Self {
            HllOperation {
                op: OperationType::HllFold(bin_name, index_bit_count),
            }
        }

        /// Create HLL get_union operation.
        /// Server returns an HLL object that is the union of all specified HLL objects
        /// in the list with the HLL bin.
        #[staticmethod]
        pub fn get_union(bin_name: String, hll_list: Vec<PythonValue>) -> Self {
            HllOperation {
                op: OperationType::HllGetUnion(bin_name, hll_list),
            }
        }

        /// Create HLL get_union_count operation.
        /// Server returns estimated number of elements that would be contained
        /// by the union of these HLL objects.
        #[staticmethod]
        pub fn get_union_count(bin_name: String, hll_list: Vec<PythonValue>) -> Self {
            HllOperation {
                op: OperationType::HllGetUnionCount(bin_name, hll_list),
            }
        }

        /// Create HLL get_intersect_count operation.
        /// Server returns estimated number of elements that would be contained
        /// by the intersection of these HLL objects.
        #[staticmethod]
        pub fn get_intersect_count(bin_name: String, hll_list: Vec<PythonValue>) -> Self {
            HllOperation {
                op: OperationType::HllGetIntersectCount(bin_name, hll_list),
            }
        }

        /// Create HLL get_similarity operation.
        /// Server returns estimated similarity of these HLL objects. Return type is a double.
        #[staticmethod]
        pub fn get_similarity(bin_name: String, hll_list: Vec<PythonValue>) -> Self {
            HllOperation {
                op: OperationType::HllGetSimilarity(bin_name, hll_list),
            }
        }

        /// Create HLL set_union operation.
        /// Server sets union of specified HLL objects with HLL bin.
        /// Server does not return a value.
        #[staticmethod]
        #[pyo3(signature = (bin_name, hll_list, flags = 0))]
        pub fn set_union(bin_name: String, hll_list: Vec<PythonValue>, flags: i64) -> Self {
            HllOperation {
                op: OperationType::HllSetUnion(bin_name, hll_list, flags),
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ExpOperation
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Expression operations. Create expression operations used by the client's `operate()` method.
    /// Expression operations allow evaluating expressions on the server and optionally storing
    /// the result in a bin.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct ExpOperation {
        pub(crate) op: OperationType,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl ExpOperation {
        /// Create expression read operation.
        ///
        /// Evaluates the expression and returns the result. The result is returned
        /// in the record bins with the specified name.
        ///
        /// Args:
        ///     name: Name to assign to the expression result in the returned record.
        ///     exp: Expression to evaluate.
        ///     flags: Expression read flags (default: ExpReadFlags.DEFAULT).
        ///
        /// Returns:
        ///     An ExpOperation to use with client.operate().
        #[staticmethod]
        #[pyo3(signature = (name, exp, flags = 0))]
        pub fn read(name: String, exp: FilterExpression, flags: i64) -> Self {
            ExpOperation {
                op: OperationType::ExpRead(name, exp, flags),
            }
        }

        /// Create expression write operation.
        ///
        /// Evaluates the expression and writes the result to the specified bin.
        ///
        /// Args:
        ///     bin_name: Name of bin to store expression result.
        ///     exp: Expression to evaluate.
        ///     flags: Expression write flags (default: ExpWriteFlags.DEFAULT).
        ///
        /// Returns:
        ///     An ExpOperation to use with client.operate().
        #[staticmethod]
        #[pyo3(signature = (bin_name, exp, flags = 0))]
        pub fn write(bin_name: String, exp: FilterExpression, flags: i64) -> Self {
            ExpOperation {
                op: OperationType::ExpWrite(bin_name, exp, flags),
            }
        }
    }
pub(crate) fn bins_flag(bins: Option<Vec<String>>) -> aerospike_core::Bins {
    match bins {
        None => aerospike_core::Bins::All,
        Some(bins) => {
            if !bins.is_empty() {
                aerospike_core::Bins::Some(bins)
            } else {
                aerospike_core::Bins::None
            }
        }
    }
}

/// Extract a list of Python operation objects into the internal `OperationType` representation.
pub(crate) fn extract_py_ops(py: Python<'_>, py_ops: &[Py<PyAny>]) -> PyResult<Vec<OperationType>> {
    let mut rust_ops = Vec::with_capacity(py_ops.len());
    for op_obj in py_ops {
        if let Ok(py_op) = op_obj.extract::<PyRef<Operation>>(py) {
            rust_ops.push(py_op.op.clone());
        } else if let Ok(py_op) = op_obj.extract::<PyRef<ListOperation>>(py) {
            rust_ops.push(py_op.op.clone());
        } else if let Ok(py_op) = op_obj.extract::<PyRef<MapOperation>>(py) {
            rust_ops.push(py_op.op.clone());
        } else if let Ok(py_op) = op_obj.extract::<PyRef<BitOperation>>(py) {
            rust_ops.push(py_op.op.clone());
        } else if let Ok(py_op) = op_obj.extract::<PyRef<HllOperation>>(py) {
            rust_ops.push(py_op.op.clone());
        } else if let Ok(py_op) = op_obj.extract::<PyRef<ExpOperation>>(py) {
            rust_ops.push(py_op.op.clone());
        } else {
            return Err(PyTypeError::new_err(
                "Operation must be Operation, ListOperation, MapOperation, BitOperation, HllOperation, or ExpOperation"
            ));
        }
    }
    Ok(rust_ops)
}

/// Convert scalar and expression operations to core `Operation` objects.
///
/// Used for background `query_operate` only. CDT operations are rejected.
///
/// Returns the converted operations and whether any of them are writes.
pub(crate) fn convert_scalar_ops_to_core(
    ops: &[OperationType],
) -> PyResult<(Vec<aerospike_core::operations::Operation>, bool)> {
    use aerospike_core::operations;
    let mut core_ops = Vec::new();
    let mut has_write = false;
    for op in ops {
        let core_op = match op {
            OperationType::Get() => operations::get(),
            OperationType::GetBin(name) => operations::get_bin(name),
            OperationType::GetHeader() => operations::get_header(),
            OperationType::Put(name, val) => {
                has_write = true;
                let bin = aerospike_core::Bin::new(name.clone(), val.clone().into());
                operations::put(&bin)
            }
            OperationType::Add(name, val) => {
                has_write = true;
                let bin = aerospike_core::Bin::new(name.clone(), val.clone().into());
                operations::add(&bin)
            }
            OperationType::Append(name, val) => {
                has_write = true;
                let bin = aerospike_core::Bin::new(name.clone(), val.clone().into());
                operations::append(&bin)
            }
            OperationType::Prepend(name, val) => {
                has_write = true;
                let bin = aerospike_core::Bin::new(name.clone(), val.clone().into());
                operations::prepend(&bin)
            }
            OperationType::Delete() => {
                has_write = true;
                operations::delete()
            }
            OperationType::Touch() => {
                has_write = true;
                operations::touch()
            }
            OperationType::ExpRead(name, exp, flags) => {
                use aerospike_core::operations::exp::{self, ExpReadFlags};
                let mut core_flags: Vec<ExpReadFlags> = Vec::new();
                if *flags & 16 != 0 {
                    core_flags.push(ExpReadFlags::EvalNoFail);
                }
                if core_flags.is_empty() {
                    exp::read_exp(name, exp._as.clone(), ExpReadFlags::Default)
                } else {
                    exp::read_exp(name, exp._as.clone(), core_flags)
                }
            }
            OperationType::ExpWrite(bin_name, exp, flags) => {
                has_write = true;
                use aerospike_core::operations::exp::{self, ExpWriteFlags};
                let mut core_flags: Vec<ExpWriteFlags> = Vec::new();
                if *flags & 1 != 0 { core_flags.push(ExpWriteFlags::CreateOnly); }
                if *flags & 2 != 0 { core_flags.push(ExpWriteFlags::UpdateOnly); }
                if *flags & 4 != 0 { core_flags.push(ExpWriteFlags::AllowDelete); }
                if *flags & 8 != 0 { core_flags.push(ExpWriteFlags::PolicyNoFail); }
                if *flags & 16 != 0 { core_flags.push(ExpWriteFlags::EvalNoFail); }
                if core_flags.is_empty() {
                    exp::write_exp(bin_name, exp._as.clone(), ExpWriteFlags::Default)
                } else {
                    exp::write_exp(bin_name, exp._as.clone(), core_flags)
                }
            }
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    format!("Operation type {other:?} is not yet supported in Client.batch(). \
                             Use batch_operate() for list, map, bit, and HLL operations.")
                ));
            }
        };
        core_ops.push(core_op);
    }
    Ok((core_ops, has_write))
}
/// Operation plus optional nested CDT context from the Python operation wrappers.
#[derive(Clone, Debug)]
pub(crate) struct OpWithCtx {
    pub op: OperationType,
    pub ctx: Option<Vec<aerospike_core::operations::cdt_context::CdtContext>>,
}

/// Extract Python operation objects into `OpWithCtx` (preserves list/map CDT context).
pub(crate) fn extract_py_ops_with_ctx(py: Python<'_>, py_ops: &[Py<PyAny>]) -> PyResult<Vec<OpWithCtx>> {
    let mut rust_ops = Vec::with_capacity(py_ops.len());
    for op_obj in py_ops {
        if let Ok(py_op) = op_obj.extract::<PyRef<Operation>>(py) {
            rust_ops.push(OpWithCtx {
                op: py_op.op.clone(),
                ctx: None,
            });
        } else if let Ok(py_op) = op_obj.extract::<PyRef<ListOperation>>(py) {
            let ctx = py_op.ctx.as_ref().map(|ctx_vec| {
                ctx_vec.iter().map(|c| c.ctx.clone()).collect()
            });
            rust_ops.push(OpWithCtx {
                op: py_op.op.clone(),
                ctx,
            });
        } else if let Ok(py_op) = op_obj.extract::<PyRef<MapOperation>>(py) {
            let ctx = py_op.ctx.as_ref().map(|ctx_vec| {
                ctx_vec.iter().map(|c| c.ctx.clone()).collect()
            });
            rust_ops.push(OpWithCtx {
                op: py_op.op.clone(),
                ctx,
            });
        } else if let Ok(py_op) = op_obj.extract::<PyRef<BitOperation>>(py) {
            rust_ops.push(OpWithCtx {
                op: py_op.op.clone(),
                ctx: None,
            });
        } else if let Ok(py_op) = op_obj.extract::<PyRef<HllOperation>>(py) {
            rust_ops.push(OpWithCtx {
                op: py_op.op.clone(),
                ctx: None,
            });
        } else if let Ok(py_op) = op_obj.extract::<PyRef<ExpOperation>>(py) {
            rust_ops.push(OpWithCtx {
                op: py_op.op.clone(),
                ctx: None,
            });
        } else {
            return Err(PyTypeError::new_err(
                "Operation must be Operation, ListOperation, MapOperation, BitOperation, HllOperation, or ExpOperation"
            ));
        }
    }
    Ok(rust_ops)
}

pub(crate) fn record_batch_ops_have_write(rust_ops: &[OpWithCtx]) -> bool {
    for owc in rust_ops {
        match &owc.op {
            OperationType::Put(_, _) | OperationType::Add(_, _) | OperationType::Append(_, _) |
            OperationType::Prepend(_, _) | OperationType::Delete() | OperationType::Touch() |
            OperationType::ListSet(_, _, _) | OperationType::ListSetWithPolicy(_, _, _, _) |
            OperationType::ListAppend(_, _, _) | OperationType::ListAppendItems(_, _, _) |
            OperationType::ListInsert(_, _, _, _) | OperationType::ListInsertItems(_, _, _, _) |
            OperationType::ListIncrement(_, _, _, _) | OperationType::ListIncrementByOne(_, _) |
            OperationType::ListIncrementByOneWithPolicy(_, _, _) |
            OperationType::ListSort(_, _) | OperationType::ListSetOrder(_, _) |
            OperationType::ListCreateWithIndex(_, _) | OperationType::ListSetOrderWithIndex(_, _) |
            OperationType::ListRemove(_, _) | OperationType::ListRemoveRange(_, _, _) |
            OperationType::ListRemoveRangeFrom(_, _) | OperationType::ListPop(_, _) |
            OperationType::ListPopRange(_, _, _) | OperationType::ListPopRangeFrom(_, _) |
            OperationType::ListTrim(_, _, _) | OperationType::ListClear(_) |
            OperationType::ListCreate(_, _, _, _) |
            OperationType::ListRemoveByIndex(_, _, _) | OperationType::ListRemoveByIndexRange(_, _, _, _) |
            OperationType::ListRemoveByRank(_, _, _) | OperationType::ListRemoveByRankRange(_, _, _, _) |
            OperationType::ListRemoveByValue(_, _, _) | OperationType::ListRemoveByValueList(_, _, _) |
            OperationType::ListRemoveByValueRange(_, _, _, _) |
            OperationType::ListRemoveByValueRelativeRankRange(_, _, _, _, _) |
            OperationType::MapPut(_, _, _, _) | OperationType::MapPutItems(_, _, _) |
            OperationType::MapIncrementValue(_, _, _, _) | OperationType::MapDecrementValue(_, _, _, _) |
            OperationType::MapClear(_) | OperationType::MapSetMapPolicy(_, _) |
            OperationType::MapSetPolicy(_, _) | OperationType::MapCreate(_, _) |
            OperationType::MapCreateWithIndex(_, _) | OperationType::MapRemoveByKey(_, _, _) |
            OperationType::MapRemoveByKeyRange(_, _, _, _) | OperationType::MapRemoveByIndex(_, _, _) |
            OperationType::MapRemoveByIndexRange(_, _, _, _) |
            OperationType::MapRemoveByIndexRangeFrom(_, _, _) | OperationType::MapRemoveByRank(_, _, _) |
            OperationType::MapRemoveByRankRange(_, _, _, _) |
            OperationType::MapRemoveByRankRangeFrom(_, _, _) | OperationType::MapRemoveByValue(_, _, _) |
            OperationType::MapRemoveByValueRange(_, _, _, _) | OperationType::MapRemoveByKeyList(_, _, _) |
            OperationType::MapRemoveByValueList(_, _, _) |
            OperationType::MapRemoveByKeyRelativeIndexRange(_, _, _, _, _) |
            OperationType::MapRemoveByValueRelativeRankRange(_, _, _, _, _) |
            OperationType::BitResize(_, _, _, _) | OperationType::BitInsert(_, _, _, _) |
            OperationType::BitRemove(_, _, _, _) | OperationType::BitSet(_, _, _, _, _) |
            OperationType::BitOr(_, _, _, _, _) | OperationType::BitXor(_, _, _, _, _) |
            OperationType::BitAnd(_, _, _, _, _) | OperationType::BitNot(_, _, _, _) |
            OperationType::BitLShift(_, _, _, _, _) | OperationType::BitRShift(_, _, _, _, _) |
            OperationType::BitAdd(_, _, _, _, _, _, _) | OperationType::BitSubtract(_, _, _, _, _, _, _) |
            OperationType::BitSetInt(_, _, _, _, _) |
            OperationType::ExpWrite(_, _, _) |
            OperationType::HllInit(_, _, _, _) | OperationType::HllAdd(_, _, _, _, _) |
            OperationType::HllFold(_, _) | OperationType::HllSetUnion(_, _, _) => {
                return true;
            }
            _ => {}
        }
    }
    false
}

/// Full operation conversion (scalar, CDT, expressions, HLL) with optional CDT context.
/// When `disallow_hll` is true, returns `NotImplementedError` if any HLL operation is present
/// (`batch_operate` policy).
pub(crate) fn convert_ops_with_ctx_to_core(
    rust_ops: &[OpWithCtx],
    disallow_hll: bool,
) -> PyResult<(Vec<aerospike_core::operations::Operation>, bool)> {

    if disallow_hll {
        for owc in rust_ops {
            match &owc.op {
                OperationType::HllInit(_, _, _, _)
                | OperationType::HllAdd(_, _, _, _, _)
                | OperationType::HllGetCount(_)
                | OperationType::HllDescribe(_)
                | OperationType::HllRefreshCount(_)
                | OperationType::HllFold(_, _)
                | OperationType::HllGetUnion(_, _)
                | OperationType::HllGetUnionCount(_, _)
                | OperationType::HllGetIntersectCount(_, _)
                | OperationType::HllGetSimilarity(_, _)
                | OperationType::HllSetUnion(_, _, _) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                        "HLL operations are not supported in batch_operate. Use operate() instead.",
                    ));
                }
                _ => {}
            }
        }
    }

    use aerospike_core::operations;

    // First pass: collect all bins/values that need to live as long as the operations
    let mut bin_storage: Vec<aerospike_core::Bin> = Vec::new();
    let mut value_storage: Vec<aerospike_core::Value> = Vec::new();
    let mut map_storage: Vec<HashMap<aerospike_core::Value, aerospike_core::Value>> = Vec::new();
    let mut list_storage: Vec<Vec<aerospike_core::Value>> = Vec::new();
    let mut hll_value_storage: Vec<Vec<aerospike_core::Value>> = Vec::new();
    for op_with_ctx in rust_ops {
        match &op_with_ctx.op {
            OperationType::Put(bin_name, value) |
            OperationType::Add(bin_name, value) |
            OperationType::Append(bin_name, value) |
            OperationType::Prepend(bin_name, value) => {
                let bin = aerospike_core::Bin::new(bin_name.clone(), value.clone().into());
                bin_storage.push(bin);
            }
            OperationType::ListSet(_, _, value) => {
                // Store the value for list_set operation
                value_storage.push(value.clone().into());
            }
            OperationType::ListSetWithPolicy(_, _, value, _) => {
                // Store the value for list set_with_policy operation
                value_storage.push(value.clone().into());
            }
            OperationType::ListAppend(_, value, _) => {
                // Store the value for list_append operation
                value_storage.push(value.clone().into());
            }
            OperationType::ListAppendItems(_, values, _) => {
                // Store all values for list_append_items operation
                for value in values {
                    value_storage.push(value.clone().into());
                }
            }
            OperationType::ListInsert(_, _, value, _) => {
                // Store the value for list_insert operation
                value_storage.push(value.clone().into());
            }
            OperationType::ListInsertItems(_, _, values, _) => {
                // Store all values for list_insert_items operation
                for value in values {
                    value_storage.push(value.clone().into());
                }
            }
            OperationType::ListGetByValue(_, value, _) => {
                // Store value for list get_by_value operation
                value_storage.push(value.clone().into());
            }
            OperationType::ListGetByValueRange(_, begin, end, _) => {
                // Store begin and end values for list get_by_value_range operation
                value_storage.push(begin.clone().into());
                value_storage.push(end.clone().into());
            }
            OperationType::ListGetByValueList(_, values, _) => {
                // Store list of values for list get_by_value_list operation
                let mut value_list = Vec::new();
                for value in values {
                    value_list.push(value.clone().into());
                }
                list_storage.push(value_list);
            }
            OperationType::ListGetByValueRelativeRankRange(_, value, _, _, _) => {
                // Store value for list get_by_value_relative_rank_range operation
                value_storage.push(value.clone().into());
            }
            OperationType::ListRemoveByValue(_, value, _) => {
                // Store value for list remove_by_value operation
                value_storage.push(value.clone().into());
            }
            OperationType::ListRemoveByValueList(_, values, _) => {
                // Store list of values for list remove_by_value_list operation
                let mut value_list = Vec::new();
                for value in values {
                    value_list.push(value.clone().into());
                }
                list_storage.push(value_list);
            }
            OperationType::ListRemoveByValueRange(_, begin, end, _) => {
                // Store begin and end values for list remove_by_value_range operation
                value_storage.push(begin.clone().into());
                value_storage.push(end.clone().into());
            }
            OperationType::ListRemoveByValueRelativeRankRange(_, value, _, _, _) => {
                // Store value for list remove_by_value_relative_rank_range operation
                value_storage.push(value.clone().into());
            }
            OperationType::MapPut(_, key, value, _) => {
                // Store key and value for map_put operation
                value_storage.push(key.clone().into());
                value_storage.push(value.clone().into());
            }
            OperationType::MapPutItems(_, items, _) => {
                // Store all keys and values for map_put_items operation
                use std::collections::HashMap;
                let mut map = HashMap::new();
                for (key, value) in items {
                    map.insert(key.clone().into(), value.clone().into());
                }
                map_storage.push(map);
            }
            OperationType::MapIncrementValue(_, key, value, _) | OperationType::MapDecrementValue(_, key, value, _) => {
                // Store key and increment/decrement value for map increment/decrement operations
                value_storage.push(key.clone().into());
                value_storage.push(aerospike_core::Value::Int(*value));
            }
            OperationType::MapGetByKey(_, key, _) | OperationType::MapRemoveByKey(_, key, _) => {
                // Store key for map get_by_key and remove_by_key operations
                value_storage.push(key.clone().into());
            }
            OperationType::MapGetByKeyRange(_, begin, end, _) | OperationType::MapRemoveByKeyRange(_, begin, end, _) => {
                // Store begin and end keys for map get_by_key_range and remove_by_key_range operations
                value_storage.push(begin.clone().into());
                value_storage.push(end.clone().into());
            }
            OperationType::MapGetByValue(_, value, _) | OperationType::MapRemoveByValue(_, value, _) => {
                // Store value for map get_by_value and remove_by_value operations
                value_storage.push(value.clone().into());
            }
            OperationType::MapGetByValueRange(_, begin, end, _) | OperationType::MapRemoveByValueRange(_, begin, end, _) => {
                // Store begin and end values for map get_by_value_range and remove_by_value_range operations
                value_storage.push(begin.clone().into());
                value_storage.push(end.clone().into());
            }
            OperationType::MapGetByKeyList(_, keys, _) | OperationType::MapRemoveByKeyList(_, keys, _) => {
                // Store list of keys for map get_by_key_list and remove_by_key_list operations
                let mut key_list = Vec::new();
                for key in keys {
                    key_list.push(key.clone().into());
                }
                list_storage.push(key_list);
            }
            OperationType::MapGetByValueList(_, values, _) | OperationType::MapRemoveByValueList(_, values, _) => {
                // Store list of values for map get_by_value_list and remove_by_value_list operations
                let mut value_list = Vec::new();
                for value in values {
                    value_list.push(value.clone().into());
                }
                list_storage.push(value_list);
            }
            OperationType::MapGetByKeyRelativeIndexRange(_, key, _, _, _) | OperationType::MapRemoveByKeyRelativeIndexRange(_, key, _, _, _) => {
                // Store key for map get_by_key_relative_index_range and remove_by_key_relative_index_range operations
                value_storage.push(key.clone().into());
            }
            OperationType::MapGetByValueRelativeRankRange(_, value, _, _, _) | OperationType::MapRemoveByValueRelativeRankRange(_, value, _, _, _) => {
                // Store value for map get_by_value_relative_rank_range and remove_by_value_relative_rank_range operations
                value_storage.push(value.clone().into());
            }
            OperationType::BitInsert(_, _, value, _) | OperationType::BitSet(_, _, _, value, _) |
            OperationType::BitOr(_, _, _, value, _) | OperationType::BitXor(_, _, _, value, _) |
            OperationType::BitAnd(_, _, _, value, _) => {
                // Store value for bit operations that require a value
                value_storage.push(value.clone().into());
            }
            // Operations that don't require storage in first pass
            OperationType::Get() | OperationType::GetBin(_) | OperationType::GetHeader() |
            OperationType::Delete() | OperationType::Touch() |
            OperationType::ListGet(_, _) | OperationType::ListSize(_) | OperationType::ListPop(_, _) |
            OperationType::ListClear(_) | OperationType::ListGetRange(_, _, _) |
            OperationType::ListRemove(_, _) | OperationType::ListRemoveRange(_, _, _) |
            OperationType::ListGetRangeFrom(_, _) | OperationType::ListPopRange(_, _, _) |
            OperationType::ListPopRangeFrom(_, _) | OperationType::ListRemoveRangeFrom(_, _) |
            OperationType::ListTrim(_, _, _) | OperationType::ListIncrement(_, _, _, _) |
            OperationType::ListIncrementByOne(_, _) | OperationType::ListIncrementByOneWithPolicy(_, _, _) |
            OperationType::ListSort(_, _) | OperationType::ListSetOrder(_, _) |
            OperationType::ListCreateWithIndex(_, _) | OperationType::ListSetOrderWithIndex(_, _) |
            OperationType::ListGetByIndex(_, _, _) | OperationType::ListGetByIndexRange(_, _, _, _) |
            OperationType::ListGetByRank(_, _, _) | OperationType::ListGetByRankRange(_, _, _, _) |
            OperationType::ListRemoveByIndex(_, _, _) | OperationType::ListRemoveByIndexRange(_, _, _, _) |
            OperationType::ListRemoveByRank(_, _, _) | OperationType::ListRemoveByRankRange(_, _, _, _) |
            OperationType::ListCreate(_, _, _, _) |
            OperationType::MapSize(_) | OperationType::MapClear(_) |
            OperationType::MapCreateWithIndex(_, _) | OperationType::MapSetPolicy(_, _) |
            OperationType::MapGetByIndex(_, _, _) | OperationType::MapRemoveByIndex(_, _, _) |
            OperationType::MapGetByIndexRange(_, _, _, _) | OperationType::MapRemoveByIndexRange(_, _, _, _) |
            OperationType::MapGetByIndexRangeFrom(_, _, _) | OperationType::MapRemoveByIndexRangeFrom(_, _, _) |
            OperationType::MapGetByRank(_, _, _) | OperationType::MapRemoveByRank(_, _, _) |
            OperationType::MapGetByRankRange(_, _, _, _) | OperationType::MapRemoveByRankRange(_, _, _, _) |
            OperationType::MapGetByRankRangeFrom(_, _, _) | OperationType::MapRemoveByRankRangeFrom(_, _, _) |
            OperationType::MapSetMapPolicy(_, _) | OperationType::MapCreate(_, _) |
            OperationType::BitResize(_, _, _, _) | OperationType::BitRemove(_, _, _, _) |
            OperationType::BitNot(_, _, _, _) | OperationType::BitLShift(_, _, _, _, _) |
            OperationType::BitRShift(_, _, _, _, _) | OperationType::BitAdd(_, _, _, _, _, _, _) |
            OperationType::BitSubtract(_, _, _, _, _, _, _) | OperationType::BitSetInt(_, _, _, _, _) |
            OperationType::BitGet(_, _, _) | OperationType::BitCount(_, _, _) |
            OperationType::BitLScan(_, _, _, _) | OperationType::BitRScan(_, _, _, _) |
            OperationType::BitGetInt(_, _, _, _) |
            OperationType::HllInit(_, _, _, _) |
            OperationType::HllGetCount(_) | OperationType::HllDescribe(_) |
            OperationType::HllRefreshCount(_) | OperationType::HllFold(_, _) => {
            }
            OperationType::HllAdd(_, values, _, _, _) => {
                let core_values: Vec<aerospike_core::Value> = values.iter().map(|v| v.clone().into()).collect();
                hll_value_storage.push(core_values);
            }
            OperationType::HllGetUnion(_, hll_list) |
            OperationType::HllGetUnionCount(_, hll_list) |
            OperationType::HllGetIntersectCount(_, hll_list) |
            OperationType::HllGetSimilarity(_, hll_list) => {
                let core_values: Vec<aerospike_core::Value> = hll_list.iter().map(|v| v.clone().into()).collect();
                hll_value_storage.push(core_values);
            }
            OperationType::HllSetUnion(_, hll_list, _) => {
                let core_values: Vec<aerospike_core::Value> = hll_list.iter().map(|v| v.clone().into()).collect();
                hll_value_storage.push(core_values);
            }
            // Expression operations don't need storage - Expression is cloned directly
            OperationType::ExpRead(_, _, _) | OperationType::ExpWrite(_, _, _) => {}
        }
    }

    // Second pass: convert operations, using references to stored bins/values
    let mut bin_idx = 0;
    let mut value_idx = 0;
    let mut map_idx = 0;
    let mut list_idx = 0;
    let mut hll_idx = 0;
    let mut core_ops: Vec<operations::Operation> = Vec::new();

    for op_with_ctx in rust_ops {
        let core_op = match &op_with_ctx.op {
            OperationType::Get() => {
                // Use the operations module's get() function to create a Get operation
                operations::get()
            }
            OperationType::GetBin(bin_name) => {
                // Use the operations module's get_bin() function to get a specific bin
                operations::get_bin(bin_name)
            }
            OperationType::GetHeader() => {
                // Use the operations module's get_header() function
                operations::get_header()
            }
            OperationType::Put(_, _) => {
                // Use a reference to the stored bin
                let op = operations::put(&bin_storage[bin_idx]);
                bin_idx += 1;
                op
            }
            OperationType::Add(_, _) => {
                // Use a reference to the stored bin
                let op = operations::add(&bin_storage[bin_idx]);
                bin_idx += 1;
                op
            }
            OperationType::Append(_, _) => {
                // Use a reference to the stored bin
                let op = operations::append(&bin_storage[bin_idx]);
                bin_idx += 1;
                op
            }
            OperationType::Prepend(_, _) => {
                // Use a reference to the stored bin
                let op = operations::prepend(&bin_storage[bin_idx]);
                bin_idx += 1;
                op
            }
            OperationType::Delete() => {
                // Use the operations module's delete() function
                operations::delete()
            }
            OperationType::Touch() => {
                // Use the operations module's touch() function
                operations::touch()
            }
            OperationType::ListGet(bin_name, index) => {
                // Use the operations module's list get() function
                use aerospike_core::operations::lists;
                lists::get(bin_name, *index)
            }
            OperationType::ListSize(bin_name) => {
                // Use the operations module's list size() function
                use aerospike_core::operations::lists;
                lists::size(bin_name)
            }
            OperationType::ListPop(bin_name, index) => {
                // Use the operations module's list pop() function
                use aerospike_core::operations::lists;
                lists::pop(bin_name, *index)
            }
            OperationType::ListClear(bin_name) => {
                // Use the operations module's list clear() function
                use aerospike_core::operations::lists;
                lists::clear(bin_name)
            }
            OperationType::ListGetRange(bin_name, index, count) => {
                // Use the operations module's list get_range() function
                use aerospike_core::operations::lists;
                lists::get_range(bin_name, *index, *count)
            }
            OperationType::ListSet(bin_name, index, _) => {
                // Use the operations module's list set() function with stored value
                use aerospike_core::operations::lists;
                let op = lists::set(bin_name, *index, value_storage[value_idx].clone());
                value_idx += 1;
                op
            }
            OperationType::ListSetWithPolicy(bin_name, index, _, policy) => {
                use aerospike_core::operations::lists;
                let op = lists::set_with_policy(&policy._as, bin_name, *index, value_storage[value_idx].clone());
                value_idx += 1;
                op
            }
            OperationType::ListIncrementByOne(bin_name, index) => {
                use aerospike_core::operations::lists;
                lists::increment_by_one(bin_name, *index)
            }
            OperationType::ListIncrementByOneWithPolicy(bin_name, index, policy) => {
                use aerospike_core::operations::lists;
                lists::increment_by_one_with_policy(&policy._as, bin_name, *index)
            }
            OperationType::ListCreateWithIndex(bin_name, order) => {
                use aerospike_core::operations::lists;
                let core_order: aerospike_core::operations::lists::ListOrderType = order.into();
                lists::create_with_index(bin_name, core_order)
            }
            OperationType::ListSetOrderWithIndex(bin_name, order) => {
                use aerospike_core::operations::lists;
                let core_order: aerospike_core::operations::lists::ListOrderType = order.into();
                lists::set_order_with_index(bin_name, core_order)
            }
            OperationType::ListRemove(bin_name, index) => {
                // Use the operations module's list remove() function
                use aerospike_core::operations::lists;
                lists::remove(bin_name, *index)
            }
            OperationType::ListRemoveRange(bin_name, index, count) => {
                // Use the operations module's list remove_range() function
                use aerospike_core::operations::lists;
                lists::remove_range(bin_name, *index, *count)
            }
            OperationType::ListGetRangeFrom(bin_name, index) => {
                // Use the operations module's list get_range_from() function
                use aerospike_core::operations::lists;
                lists::get_range_from(bin_name, *index)
            }
            OperationType::ListPopRange(bin_name, index, count) => {
                // Use the operations module's list pop_range() function
                use aerospike_core::operations::lists;
                lists::pop_range(bin_name, *index, *count)
            }
            OperationType::ListPopRangeFrom(bin_name, index) => {
                // Use the operations module's list pop_range_from() function
                use aerospike_core::operations::lists;
                lists::pop_range_from(bin_name, *index)
            }
            OperationType::ListRemoveRangeFrom(bin_name, index) => {
                // Use the operations module's list remove_range_from() function
                use aerospike_core::operations::lists;
                lists::remove_range_from(bin_name, *index)
            }
            OperationType::ListTrim(bin_name, index, count) => {
                // Use the operations module's list trim() function
                use aerospike_core::operations::lists;
                lists::trim(bin_name, *index, *count)
            }
            OperationType::ListAppend(bin_name, _, policy) => {
                // Use the operations module's list append() function with stored value and policy
                use aerospike_core::operations::lists;
                let op = lists::append(&policy._as, bin_name, value_storage[value_idx].clone());
                value_idx += 1;
                op
            }
            OperationType::ListAppendItems(bin_name, values, policy) => {
                // Use the operations module's list append_items() function with stored values and policy
                use aerospike_core::operations::lists;
                let values_slice: &[aerospike_core::Value] = &value_storage[value_idx..value_idx + values.len()];
                let op = lists::append_items(&policy._as, bin_name, values_slice.to_vec());
                value_idx += values.len();
                op
            }
            OperationType::ListInsert(bin_name, index, _, policy) => {
                // Use the operations module's list insert() function with stored value and policy
                use aerospike_core::operations::lists;
                let op = lists::insert(&policy._as, bin_name, *index, value_storage[value_idx].clone());
                value_idx += 1;
                op
            }
            OperationType::ListInsertItems(bin_name, index, values, policy) => {
                // Use the operations module's list insert_items() function with stored values and policy
                use aerospike_core::operations::lists;
                let values_slice: &[aerospike_core::Value] = &value_storage[value_idx..value_idx + values.len()];
                let op = lists::insert_items(&policy._as, bin_name, *index, values_slice.to_vec());
                value_idx += values.len();
                op
            }
            OperationType::ListIncrement(bin_name, index, value, policy) => {
                // Use the operations module's list increment() function with policy
                use aerospike_core::operations::lists;
                lists::increment(&policy._as, bin_name, *index, *value)
            }
            OperationType::ListSort(bin_name, flags) => {
                // Use the operations module's list sort() function
                use aerospike_core::operations::lists;
                let core_flags: aerospike_core::operations::lists::ListSortFlags = flags.into();
                lists::sort(bin_name, core_flags)
            }
            OperationType::ListSetOrder(bin_name, order) => {
                // Use the operations module's list set_order() function
                use aerospike_core::operations::lists;
                let core_order: aerospike_core::operations::lists::ListOrderType = order.into();
                lists::set_order(bin_name, core_order)
            }
            OperationType::ListGetByIndex(bin_name, index, return_type) => {
                // Use the operations module's list get_by_index() function with return type
                use aerospike_core::operations::lists;
                let core_return_type = *return_type;
                lists::get_by_index(bin_name, *index, core_return_type)
            }
            OperationType::ListGetByIndexRange(bin_name, index, count, return_type) => {
                // Use the operations module's list get_by_index_range() or get_by_index_range_count() function
                use aerospike_core::operations::lists;
                let core_return_type = *return_type;
                match count {
                    Some(c) => lists::get_by_index_range_count(bin_name, *index, *c, core_return_type),
                    None => lists::get_by_index_range(bin_name, *index, core_return_type),
                }
            }
            OperationType::ListGetByRank(bin_name, rank, return_type) => {
                // Use the operations module's list get_by_rank() function with return type
                use aerospike_core::operations::lists;
                let core_return_type = *return_type;
                lists::get_by_rank(bin_name, *rank, core_return_type)
            }
            OperationType::ListGetByRankRange(bin_name, rank, count, return_type) => {
                // Use the operations module's list get_by_rank_range() or get_by_rank_range_count() function
                use aerospike_core::operations::lists;
                let core_return_type = *return_type;
                match count {
                    Some(c) => lists::get_by_rank_range_count(bin_name, *rank, *c, core_return_type),
                    None => lists::get_by_rank_range(bin_name, *rank, core_return_type),
                }
            }
            OperationType::ListGetByValue(bin_name, _, return_type) => {
                // Use the operations module's list get_by_value() function with stored value and return type
                use aerospike_core::operations::lists;
                let value = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = lists::get_by_value(bin_name, value.clone(), core_return_type);
                value_idx += 1;
                op
            }
            OperationType::ListGetByValueRange(bin_name, _, _, return_type) => {
                // Use the operations module's list get_by_value_range() function with stored values and return type
                use aerospike_core::operations::lists;
                let begin = &value_storage[value_idx];
                let end = &value_storage[value_idx + 1];
                let core_return_type = *return_type;
                let op = lists::get_by_value_range(bin_name, begin.clone(), end.clone(), core_return_type);
                value_idx += 2;
                op
            }
            OperationType::ListGetByValueList(bin_name, _, return_type) => {
                // Use the operations module's list get_by_value_list() function with stored list and return type
                use aerospike_core::operations::lists;
                let values = &list_storage[list_idx];
                let core_return_type = *return_type;
                let op = lists::get_by_value_list(bin_name, values.to_vec(), core_return_type);
                list_idx += 1;
                op
            }
            OperationType::ListGetByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                // Use the operations module's list get_by_value_relative_rank_range() function
                use aerospike_core::operations::lists;
                let value = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = match count {
                    Some(c) => lists::get_by_value_relative_rank_range_count(bin_name, value.clone(), *rank, *c, core_return_type),
                    None => lists::get_by_value_relative_rank_range(bin_name, value.clone(), *rank, core_return_type),
                };
                value_idx += 1;
                op
            }
            OperationType::ListRemoveByIndex(bin_name, index, return_type) => {
                // Use the operations module's list remove_by_index() function with return type
                use aerospike_core::operations::lists;
                let core_return_type = *return_type;
                lists::remove_by_index(bin_name, *index, core_return_type)
            }
            OperationType::ListRemoveByIndexRange(bin_name, index, count, return_type) => {
                // Use the operations module's list remove_by_index_range() or remove_by_index_range_count() function
                use aerospike_core::operations::lists;
                let core_return_type = *return_type;
                match count {
                    Some(c) => lists::remove_by_index_range_count(bin_name, *index, *c, core_return_type),
                    None => lists::remove_by_index_range(bin_name, *index, core_return_type),
                }
            }
            OperationType::ListRemoveByRank(bin_name, rank, return_type) => {
                // Use the operations module's list remove_by_rank() function with return type
                use aerospike_core::operations::lists;
                let core_return_type = *return_type;
                lists::remove_by_rank(bin_name, *rank, core_return_type)
            }
            OperationType::ListRemoveByRankRange(bin_name, rank, count, return_type) => {
                // Use the operations module's list remove_by_rank_range() or remove_by_rank_range_count() function
                use aerospike_core::operations::lists;
                let core_return_type = *return_type;
                match count {
                    Some(c) => lists::remove_by_rank_range_count(bin_name, *rank, *c, core_return_type),
                    None => lists::remove_by_rank_range(bin_name, *rank, core_return_type),
                }
            }
            OperationType::ListRemoveByValue(bin_name, _, return_type) => {
                // Use the operations module's list remove_by_value() function with stored value and return type
                use aerospike_core::operations::lists;
                let value = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = lists::remove_by_value(bin_name, value.clone(), core_return_type);
                value_idx += 1;
                op
            }
            OperationType::ListRemoveByValueList(bin_name, _, return_type) => {
                // Use the operations module's list remove_by_value_list() function with stored list and return type
                use aerospike_core::operations::lists;
                let values = &list_storage[list_idx];
                let core_return_type = *return_type;
                let op = lists::remove_by_value_list(bin_name, values.to_vec(), core_return_type);
                list_idx += 1;
                op
            }
            OperationType::ListRemoveByValueRange(bin_name, _, _, return_type) => {
                // Use the operations module's list remove_by_value_range() function with stored values and return type
                // Note: parameter order is (bin, return_type, begin, end)
                use aerospike_core::operations::lists;
                let begin = &value_storage[value_idx];
                let end = &value_storage[value_idx + 1];
                let core_return_type = *return_type;
                let op = lists::remove_by_value_range(bin_name, core_return_type, begin.clone(), end.clone());
                value_idx += 2;
                op
            }
            OperationType::ListRemoveByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                // Use the operations module's list remove_by_value_relative_rank_range() function
                // Note: parameter order is (bin, return_type, value, rank) for no-count version
                use aerospike_core::operations::lists;
                let value = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = match count {
                    Some(c) => lists::remove_by_value_relative_rank_range_count(bin_name, core_return_type, value.clone(), *rank, *c),
                    None => lists::remove_by_value_relative_rank_range(bin_name, core_return_type, value.clone(), *rank),
                };
                value_idx += 1;
                op
            }
            OperationType::ListCreate(bin_name, order, pad, _persist_index) => {
                // Use the operations module's list create() function
                // Note: Rust core client doesn't support persist_index parameter
                use aerospike_core::operations::lists;
                let core_order: aerospike_core::operations::lists::ListOrderType = order.into();
                lists::create(bin_name, core_order, *pad)
            }
            OperationType::MapSize(bin_name) => {
                // Use the operations module's map size() function
                use aerospike_core::operations::maps;
                maps::size(bin_name)
            }
            OperationType::MapClear(bin_name) => {
                // Use the operations module's map clear() function
                use aerospike_core::operations::maps;
                maps::clear(bin_name)
            }
            OperationType::MapPut(bin_name, _, _, policy) => {
                // Use the operations module's map put() function with stored key, value, and policy
                use aerospike_core::operations::maps;
                let key = &value_storage[value_idx];
                let value = &value_storage[value_idx + 1];
                let op = maps::put(&policy._as, bin_name, key.clone(), value.clone());
                value_idx += 2;
                op
            }
            OperationType::MapPutItems(bin_name, _, policy) => {
                // Use the operations module's map put_items() function with stored items and policy
                use aerospike_core::operations::maps;
                let op = maps::put_items(&policy._as, bin_name, map_storage[map_idx].clone());
                map_idx += 1;
                op
            }
            OperationType::MapIncrementValue(bin_name, _, _value, policy) => {
                // Use the operations module's map increment_value() function with stored key, value, and policy
                use aerospike_core::operations::maps;
                let key = &value_storage[value_idx];
                let incr_value = &value_storage[value_idx + 1];
                let op = maps::increment_value(&policy._as, bin_name, key.clone(), incr_value.clone());
                value_idx += 2;
                op
            }
            OperationType::MapDecrementValue(bin_name, _, _value, policy) => {
                // Use the operations module's map decrement_value() function with stored key, value, and policy
                use aerospike_core::operations::maps;
                let key = &value_storage[value_idx];
                let decr_value = &value_storage[value_idx + 1];
                let op = maps::decrement_value(&policy._as, bin_name, key.clone(), decr_value.clone());
                value_idx += 2;
                op
            }
            OperationType::MapGetByKey(bin_name, _, return_type) => {
                // Use the operations module's map get_by_key() function with stored key and return type
                use aerospike_core::operations::maps;
                let key = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = maps::get_by_key(bin_name, key.clone(), core_return_type);
                value_idx += 1;
                op
            }
            OperationType::MapRemoveByKey(bin_name, _, return_type) => {
                // Use the operations module's map remove_by_key() function with stored key and return type
                use aerospike_core::operations::maps;
                let key = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = maps::remove_by_key(bin_name, key.clone(), core_return_type);
                value_idx += 1;
                op
            }
            OperationType::MapGetByKeyRange(bin_name, _, _, return_type) => {
                // Use the operations module's map get_by_key_range() function with stored keys and return type
                use aerospike_core::operations::maps;
                let begin = &value_storage[value_idx];
                let end = &value_storage[value_idx + 1];
                let core_return_type = *return_type;
                let op = maps::get_by_key_range(bin_name, begin.clone(), end.clone(), core_return_type);
                value_idx += 2;
                op
            }
            OperationType::MapRemoveByKeyRange(bin_name, _, _, return_type) => {
                // Use the operations module's map remove_by_key_range() function with stored keys and return type
                use aerospike_core::operations::maps;
                let begin = &value_storage[value_idx];
                let end = &value_storage[value_idx + 1];
                let core_return_type = *return_type;
                let op = maps::remove_by_key_range(bin_name, begin.clone(), end.clone(), core_return_type);
                value_idx += 2;
                op
            }
            OperationType::MapGetByIndex(bin_name, index, return_type) => {
                // Use the operations module's map get_by_index() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::get_by_index(bin_name, *index, core_return_type)
            }
            OperationType::MapRemoveByIndex(bin_name, index, return_type) => {
                // Use the operations module's map remove_by_index() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::remove_by_index(bin_name, *index, core_return_type)
            }
            OperationType::MapGetByIndexRange(bin_name, index, count, return_type) => {
                // Use the operations module's map get_by_index_range() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::get_by_index_range(bin_name, *index, *count, core_return_type)
            }
            OperationType::MapRemoveByIndexRange(bin_name, index, count, return_type) => {
                // Use the operations module's map remove_by_index_range() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::remove_by_index_range(bin_name, *index, *count, core_return_type)
            }
            OperationType::MapGetByIndexRangeFrom(bin_name, index, return_type) => {
                // Use the operations module's map get_by_index_range_from() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::get_by_index_range_from(bin_name, *index, core_return_type)
            }
            OperationType::MapRemoveByIndexRangeFrom(bin_name, index, return_type) => {
                // Use the operations module's map remove_by_index_range_from() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::remove_by_index_range_from(bin_name, *index, core_return_type)
            }
            OperationType::MapGetByRank(bin_name, rank, return_type) => {
                // Use the operations module's map get_by_rank() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::get_by_rank(bin_name, *rank, core_return_type)
            }
            OperationType::MapRemoveByRank(bin_name, rank, return_type) => {
                // Use the operations module's map remove_by_rank() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::remove_by_rank(bin_name, *rank, core_return_type)
            }
            OperationType::MapGetByRankRange(bin_name, rank, count, return_type) => {
                // Use the operations module's map get_by_rank_range() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::get_by_rank_range(bin_name, *rank, *count, core_return_type)
            }
            OperationType::MapRemoveByRankRange(bin_name, rank, count, return_type) => {
                // Use the operations module's map remove_by_rank_range() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::remove_by_rank_range(bin_name, *rank, *count, core_return_type)
            }
            OperationType::MapGetByRankRangeFrom(bin_name, rank, return_type) => {
                // Use the operations module's map get_by_rank_range_from() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::get_by_rank_range_from(bin_name, *rank, core_return_type)
            }
            OperationType::MapRemoveByRankRangeFrom(bin_name, rank, return_type) => {
                // Use the operations module's map remove_by_rank_range_from() function with return type
                use aerospike_core::operations::maps;
                let core_return_type = *return_type;
                maps::remove_by_rank_range_from(bin_name, *rank, core_return_type)
            }
            OperationType::MapGetByValue(bin_name, _, return_type) => {
                // Use the operations module's map get_by_value() function with stored value and return type
                use aerospike_core::operations::maps;
                let value = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = maps::get_by_value(bin_name, value.clone(), core_return_type);
                value_idx += 1;
                op
            }
            OperationType::MapRemoveByValue(bin_name, _, return_type) => {
                // Use the operations module's map remove_by_value() function with stored value and return type
                use aerospike_core::operations::maps;
                let value = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = maps::remove_by_value(bin_name, value.clone(), core_return_type);
                value_idx += 1;
                op
            }
            OperationType::MapGetByValueRange(bin_name, _, _, return_type) => {
                // Use the operations module's map get_by_value_range() function with stored values and return type
                use aerospike_core::operations::maps;
                let begin = &value_storage[value_idx];
                let end = &value_storage[value_idx + 1];
                let core_return_type = *return_type;
                let op = maps::get_by_value_range(bin_name, begin.clone(), end.clone(), core_return_type);
                value_idx += 2;
                op
            }
            OperationType::MapRemoveByValueRange(bin_name, _, _, return_type) => {
                // Use the operations module's map remove_by_value_range() function with stored values and return type
                use aerospike_core::operations::maps;
                let begin = &value_storage[value_idx];
                let end = &value_storage[value_idx + 1];
                let core_return_type = *return_type;
                let op = maps::remove_by_value_range(bin_name, begin.clone(), end.clone(), core_return_type);
                value_idx += 2;
                op
            }
            OperationType::MapGetByKeyList(bin_name, _, return_type) => {
                // Use the operations module's map get_by_key_list() function with stored key list and return type
                use aerospike_core::operations::maps;
                let keys = &list_storage[list_idx];
                let core_return_type = *return_type;
                let op = maps::get_by_key_list(bin_name, keys.to_vec(), core_return_type);
                list_idx += 1;
                op
            }
            OperationType::MapRemoveByKeyList(bin_name, _, return_type) => {
                // Use the operations module's map remove_by_key_list() function with stored key list and return type
                use aerospike_core::operations::maps;
                let keys = &list_storage[list_idx];
                let core_return_type = *return_type;
                let op = maps::remove_by_key_list(bin_name, keys.to_vec(), core_return_type);
                list_idx += 1;
                op
            }
            OperationType::MapGetByValueList(bin_name, _, return_type) => {
                // Use the operations module's map get_by_value_list() function with stored value list and return type
                use aerospike_core::operations::maps;
                let values = &list_storage[list_idx];
                let core_return_type = *return_type;
                let op = maps::get_by_value_list(bin_name, values.to_vec(), core_return_type);
                list_idx += 1;
                op
            }
            OperationType::MapRemoveByValueList(bin_name, _, return_type) => {
                // Use the operations module's map remove_by_value_list() function with stored value list and return type
                use aerospike_core::operations::maps;
                let values = &list_storage[list_idx];
                let core_return_type = *return_type;
                let op = maps::remove_by_value_list(bin_name, values.to_vec(), core_return_type);
                list_idx += 1;
                op
            }
            OperationType::MapSetMapPolicy(bin_name, policy) => {
                // Use the operations module's map set_order() function
                // Note: Rust core client only has set_order, not full setMapPolicy
                // This sets the map order from the policy
                use aerospike_core::operations::maps;
                let core_order = policy._as.order;
                maps::set_order(bin_name, core_order)
            }
            OperationType::MapSetPolicy(bin_name, policy) => {
                use aerospike_core::operations::maps;
                maps::set_policy(&policy._as, bin_name, vec![])
            }
            OperationType::MapCreateWithIndex(bin_name, order) => {
                use aerospike_core::operations::maps;
                let core_order: aerospike_core::operations::maps::MapOrder = order.into();
                maps::create_with_index(bin_name, core_order)
            }
            OperationType::MapGetByKeyRelativeIndexRange(bin_name, _, index, count, return_type) => {
                // Use the operations module's map get_by_key_relative_index_range() function
                use aerospike_core::operations::maps;
                let key = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = match count {
                    Some(c) => maps::get_by_key_relative_index_range_count(bin_name, key.clone(), *index, *c, core_return_type),
                    None => maps::get_by_key_relative_index_range(bin_name, key.clone(), *index, core_return_type),
                };
                value_idx += 1;
                op
            }
            OperationType::MapGetByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                // Use the operations module's map get_by_value_relative_rank_range() function
                use aerospike_core::operations::maps;
                let value = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = match count {
                    Some(c) => maps::get_by_value_relative_rank_range_count(bin_name, value.clone(), *rank, *c, core_return_type),
                    None => maps::get_by_value_relative_rank_range(bin_name, value.clone(), *rank, core_return_type),
                };
                value_idx += 1;
                op
            }
            OperationType::MapRemoveByKeyRelativeIndexRange(bin_name, _, index, count, return_type) => {
                // Use the operations module's map remove_by_key_relative_index_range() function
                use aerospike_core::operations::maps;
                let key = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = match count {
                    Some(c) => maps::remove_by_key_relative_index_range_count(bin_name, key.clone(), *index, *c, core_return_type),
                    None => maps::remove_by_key_relative_index_range(bin_name, key.clone(), *index, core_return_type),
                };
                value_idx += 1;
                op
            }
            OperationType::MapRemoveByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                // Use the operations module's map remove_by_value_relative_rank_range() function
                use aerospike_core::operations::maps;
                let value = &value_storage[value_idx];
                let core_return_type = *return_type;
                let op = match count {
                    Some(c) => maps::remove_by_value_relative_rank_range_count(bin_name, value.clone(), *rank, *c, core_return_type),
                    None => maps::remove_by_value_relative_rank_range(bin_name, value.clone(), *rank, core_return_type),
                };
                value_idx += 1;
                op
            }
            OperationType::MapCreate(bin_name, order) => {
                // Use the operations module's map set_order() function
                // Note: Rust core client uses set_order instead of create for maps
                use aerospike_core::operations::maps;
                let core_order: aerospike_core::operations::maps::MapOrder = order.into();
                maps::set_order(bin_name, core_order)
            }
            OperationType::BitResize(bin_name, byte_size, resize_flags, policy) => {
                use aerospike_core::operations::bitwise;
                let flags = resize_flags.map(|f| f.into());
                bitwise::resize(bin_name, *byte_size, flags, &policy._as)
            }
            OperationType::BitInsert(bin_name, byte_offset, _, policy) => {
                use aerospike_core::operations::bitwise;
                let value = &value_storage[value_idx];
                let op = bitwise::insert(bin_name, *byte_offset, value.clone(), &policy._as);
                value_idx += 1;
                op
            }
            OperationType::BitRemove(bin_name, byte_offset, byte_size, policy) => {
                use aerospike_core::operations::bitwise;
                bitwise::remove(bin_name, *byte_offset, *byte_size, &policy._as)
            }
            OperationType::BitSet(bin_name, bit_offset, bit_size, _, policy) => {
                use aerospike_core::operations::bitwise;
                let value = &value_storage[value_idx];
                let op = bitwise::set(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
                value_idx += 1;
                op
            }
            OperationType::BitOr(bin_name, bit_offset, bit_size, _, policy) => {
                use aerospike_core::operations::bitwise;
                let value = &value_storage[value_idx];
                let op = bitwise::or(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
                value_idx += 1;
                op
            }
            OperationType::BitXor(bin_name, bit_offset, bit_size, _, policy) => {
                use aerospike_core::operations::bitwise;
                let value = &value_storage[value_idx];
                let op = bitwise::xor(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
                value_idx += 1;
                op
            }
            OperationType::BitAnd(bin_name, bit_offset, bit_size, _, policy) => {
                use aerospike_core::operations::bitwise;
                let value = &value_storage[value_idx];
                let op = bitwise::and(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
                value_idx += 1;
                op
            }
            OperationType::BitNot(bin_name, bit_offset, bit_size, policy) => {
                use aerospike_core::operations::bitwise;
                bitwise::not(bin_name, *bit_offset, *bit_size, &policy._as)
            }
            OperationType::BitLShift(bin_name, bit_offset, bit_size, shift, policy) => {
                use aerospike_core::operations::bitwise;
                bitwise::lshift(bin_name, *bit_offset, *bit_size, *shift, &policy._as)
            }
            OperationType::BitRShift(bin_name, bit_offset, bit_size, shift, policy) => {
                use aerospike_core::operations::bitwise;
                bitwise::rshift(bin_name, *bit_offset, *bit_size, *shift, &policy._as)
            }
            OperationType::BitAdd(bin_name, bit_offset, bit_size, value, signed, action, policy) => {
                use aerospike_core::operations::bitwise;
                let core_action: aerospike_core::operations::bitwise::BitwiseOverflowActions = (*action).into();
                bitwise::add(bin_name, *bit_offset, *bit_size, *value, *signed, core_action, &policy._as)
            }
            OperationType::BitSubtract(bin_name, bit_offset, bit_size, value, signed, action, policy) => {
                use aerospike_core::operations::bitwise;
                let core_action: aerospike_core::operations::bitwise::BitwiseOverflowActions = (*action).into();
                bitwise::subtract(bin_name, *bit_offset, *bit_size, *value, *signed, core_action, &policy._as)
            }
            OperationType::BitSetInt(bin_name, bit_offset, bit_size, value, policy) => {
                use aerospike_core::operations::bitwise;
                bitwise::set_int(bin_name, *bit_offset, *bit_size, *value, &policy._as)
            }
            OperationType::BitGet(bin_name, bit_offset, bit_size) => {
                use aerospike_core::operations::bitwise;
                bitwise::get(bin_name, *bit_offset, *bit_size)
            }
            OperationType::BitCount(bin_name, bit_offset, bit_size) => {
                use aerospike_core::operations::bitwise;
                bitwise::count(bin_name, *bit_offset, *bit_size)
            }
            OperationType::BitLScan(bin_name, bit_offset, bit_size, value) => {
                use aerospike_core::operations::bitwise;
                bitwise::lscan(bin_name, *bit_offset, *bit_size, *value)
            }
            OperationType::BitRScan(bin_name, bit_offset, bit_size, value) => {
                use aerospike_core::operations::bitwise;
                bitwise::rscan(bin_name, *bit_offset, *bit_size, *value)
            }
            OperationType::BitGetInt(bin_name, bit_offset, bit_size, signed) => {
                use aerospike_core::operations::bitwise;
                bitwise::get_int(bin_name, *bit_offset, *bit_size, *signed)
            }
            OperationType::HllInit(bin_name, index_bit_count, min_hash_bit_count, flags) => {
                use aerospike_core::operations::hll;
                let policy = hll::HLLPolicy { flags: *flags };
                hll::init_with_min_hash(&policy, bin_name, *index_bit_count, *min_hash_bit_count)
            }
            OperationType::HllAdd(bin_name, _, index_bit_count, min_hash_bit_count, flags) => {
                use aerospike_core::operations::hll;
                let policy = hll::HLLPolicy { flags: *flags };
                let values_ref = &hll_value_storage[hll_idx];
                hll_idx += 1;
                hll::add_with_index_and_min_hash(&policy, bin_name, values_ref.to_vec(), *index_bit_count, *min_hash_bit_count)
            }
            OperationType::HllGetCount(bin_name) => {
                use aerospike_core::operations::hll;
                hll::get_count(bin_name)
            }
            OperationType::HllDescribe(bin_name) => {
                use aerospike_core::operations::hll;
                hll::describe(bin_name)
            }
            OperationType::HllRefreshCount(bin_name) => {
                use aerospike_core::operations::hll;
                hll::refresh_count(bin_name)
            }
            OperationType::HllFold(bin_name, index_bit_count) => {
                use aerospike_core::operations::hll;
                hll::fold(bin_name, *index_bit_count)
            }
            OperationType::HllGetUnion(bin_name, _) => {
                use aerospike_core::operations::hll;
                let values_ref = &hll_value_storage[hll_idx];
                hll_idx += 1;
                hll::get_union(bin_name, values_ref.to_vec())
            }
            OperationType::HllGetUnionCount(bin_name, _) => {
                use aerospike_core::operations::hll;
                let values_ref = &hll_value_storage[hll_idx];
                hll_idx += 1;
                hll::get_union_count(bin_name, values_ref.to_vec())
            }
            OperationType::HllGetIntersectCount(bin_name, _) => {
                use aerospike_core::operations::hll;
                let values_ref = &hll_value_storage[hll_idx];
                hll_idx += 1;
                hll::get_intersect_count(bin_name, values_ref.to_vec())
            }
            OperationType::HllGetSimilarity(bin_name, _) => {
                use aerospike_core::operations::hll;
                let values_ref = &hll_value_storage[hll_idx];
                hll_idx += 1;
                hll::get_similarity(bin_name, values_ref.to_vec())
            }
            OperationType::HllSetUnion(bin_name, _, flags) => {
                use aerospike_core::operations::hll;
                let policy = hll::HLLPolicy { flags: *flags };
                let values_ref = &hll_value_storage[hll_idx];
                hll_idx += 1;
                hll::set_union(&policy, bin_name, values_ref.to_vec())
            }
            OperationType::ExpRead(name, exp, flags) => {
                use aerospike_core::operations::exp::{self, ExpReadFlags};
                // Convert flags bitmask to core ExpReadFlags
                let mut core_flags: Vec<ExpReadFlags> = Vec::new();
                if *flags & 16 != 0 {
                    core_flags.push(ExpReadFlags::EvalNoFail);
                }
                if core_flags.is_empty() {
                    exp::read_exp(name, exp._as.clone(), ExpReadFlags::Default)
                } else {
                    exp::read_exp(name, exp._as.clone(), core_flags)
                }
            }
            OperationType::ExpWrite(bin_name, exp, flags) => {
                use aerospike_core::operations::exp::{self, ExpWriteFlags};
                // Convert flags bitmask to core ExpWriteFlags
                let mut core_flags: Vec<ExpWriteFlags> = Vec::new();
                if *flags & 1 != 0 {
                    core_flags.push(ExpWriteFlags::CreateOnly);
                }
                if *flags & 2 != 0 {
                    core_flags.push(ExpWriteFlags::UpdateOnly);
                }
                if *flags & 4 != 0 {
                    core_flags.push(ExpWriteFlags::AllowDelete);
                }
                if *flags & 8 != 0 {
                    core_flags.push(ExpWriteFlags::PolicyNoFail);
                }
                if *flags & 16 != 0 {
                    core_flags.push(ExpWriteFlags::EvalNoFail);
                }
                if core_flags.is_empty() {
                    exp::write_exp(bin_name, exp._as.clone(), ExpWriteFlags::Default)
                } else {
                    exp::write_exp(bin_name, exp._as.clone(), core_flags)
                }
            }
        };

        // Apply context if present
        let final_op = if let Some(ctx) = &op_with_ctx.ctx {
            core_op.context(ctx.as_slice().to_vec())
        } else {
            core_op
        };
        core_ops.push(final_op);
    }

    let has_write = record_batch_ops_have_write(rust_ops);
    Ok((core_ops, has_write))
}
