# Copyright 2023-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""
Vector bin (``VECTOR`` particle type) round-trip integration tests.

Vector similarity search (Top-K / distance expressions) is out of scope here
— it is still work-in-progress server-side (see the README's "Vector bins and
Top-K queries" section) and covered separately. This file only exercises
storing and retrieving `Vector` bins: construction, put/get, element-type
coverage, special float values, and nesting in list/map bins.

Vector bins are an unreleased, dev-server-only feature. Support is gated via
the `supports_vector_bins` fixture in conftest.py -- see the TODO on that
fixture: it is an interim heuristic (reuses the 8.1.3 floor because current
dev builds report that version), not a real assigned capability floor. Point
`AEROSPIKE_HOST` at such a build to run these; they skip cleanly otherwise.
"""

import pytest
import pytest_asyncio

from aerospike_async import (
    BatchPolicy,
    BatchWritePolicy,
    ClientPolicy,
    Key,
    List,
    Operation,
    Vector,
    VectorElementType,
    WritePolicy,
    new_client,
)
from aerospike_async.exceptions import ResultCode


@pytest_asyncio.fixture(autouse=True)
async def _skip_without_vector_support(supports_vector_bins):
    if not supports_vector_bins:
        pytest.skip("cluster does not support VECTOR bins (requires a dev server build)")


@pytest_asyncio.fixture
async def client_and_key(aerospike_host, use_services_alternate):
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)
    key = Key("test", "vector_test", "vt")
    wp = WritePolicy()
    await client.delete(key, policy=wp)
    yield client, key, wp
    await client.delete(key, policy=wp)
    await client.close()


class TestVectorListRoundTrip:
    """Each element type, constructed from a plain Python list."""

    async def test_float32_round_trips(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([0.1, -2.5, 3.375], VectorElementType.FLOAT32)

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["v"] == v
        assert rec.bins["v"].element_type == VectorElementType.FLOAT32

    async def test_float64_round_trips(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([0.1, -2.5, 3.375, 1e300], VectorElementType.FLOAT64)

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["v"] == v
        assert rec.bins["v"].element_type == VectorElementType.FLOAT64

    async def test_int32_round_trips(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([-5, 0, 7, -2147483648, 2147483647], VectorElementType.INT32)

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["v"] == v
        assert rec.bins["v"].value == [-5, 0, 7, -2147483648, 2147483647]

    async def test_default_element_type_round_trips(self, client_and_key):
        """No explicit element_type defaults to FLOAT32, both client-side and
        after a round trip through the server."""
        client, key, wp = client_and_key
        v = Vector([0.12, 0.98, -0.34])

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["v"].element_type == VectorElementType.FLOAT32
        assert rec.bins["v"] == v


class TestVectorNumpyRoundTrip:
    """Every element type, constructed from a numpy array."""

    @pytest.mark.parametrize(
        "np_dtype, element_type, values",
        [
            ("float32", VectorElementType.FLOAT32, [0.5, -1.5, 2.0]),
            ("float64", VectorElementType.FLOAT64, [0.25, -0.5, 1.0]),
            ("int32", VectorElementType.INT32, [1, -2, 3]),
        ],
    )
    async def test_numpy_input_round_trips(self, client_and_key, np_dtype, element_type, values):
        np = pytest.importorskip("numpy")
        client, key, wp = client_and_key
        v = Vector(np.array(values, dtype=np_dtype))

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)
        got = rec.bins["v"]

        assert got.element_type == element_type
        assert got.value == values
        assert got.numpy_value.dtype == np.dtype(np_dtype)

    async def test_float16_round_trips(self, client_and_key):
        """FLOAT16 can only be built from (and read back via) numpy."""
        np = pytest.importorskip("numpy")
        client, key, wp = client_and_key
        v = Vector(np.array([1.0, -2.5, 3.5], dtype=np.float16))

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)
        got = rec.bins["v"]

        assert got == v
        assert got.element_type == VectorElementType.FLOAT16
        assert got.numpy_value.tolist() == [1.0, -2.5, 3.5]
        with pytest.raises(TypeError):
            _ = got.value


class TestVectorSpecialValues:
    """Non-finite elements and signed zero survive a round trip bit-for-bit."""

    @pytest.mark.parametrize("np_dtype", ["float16", "float32", "float64"])
    async def test_non_finite_values_round_trip_bit_exact(self, client_and_key, np_dtype):
        np = pytest.importorskip("numpy")
        client, key, wp = client_and_key
        arr = np.array([np.nan, np.inf, -np.inf, 0.0, -0.0, 1.5], dtype=np_dtype)
        v = Vector(arr)

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)
        got = rec.bins["v"].numpy_value

        assert got.tobytes() == arr.tobytes()


class TestVectorMultiBinAndAbsence:
    async def test_multiple_vector_bins_in_one_record(self, client_and_key):
        client, key, wp = client_and_key
        v1 = Vector([1.0, 2.0], VectorElementType.FLOAT32)
        v2 = Vector([1, 2, 3], VectorElementType.INT32)

        await client.put(key, {"a": v1, "b": v2, "scalar": 42}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["a"] == v1
        assert rec.bins["b"] == v2
        assert rec.bins["scalar"] == 42

    async def test_absent_vector_bin_is_not_materialized_as_empty(self, client_and_key):
        """A record with no vector bin must not surface one (empty vectors
        cannot exist at all now, but this also guards against `None`/absent
        being conflated with a zero-dimension vector)."""
        client, key, wp = client_and_key

        await client.put(key, {"scalar": 1}, policy=wp)
        rec = await client.get(key)

        assert "v" not in rec.bins
        assert rec.bins["scalar"] == 1


class TestVectorNested:
    async def test_vector_nested_in_list_bin(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([1.0, 2.0, 3.0], VectorElementType.FLOAT32)

        await client.put(key, {"l": List([1, v, "x"])}, policy=wp)
        rec = await client.get(key)
        got = rec.bins["l"]

        assert got[0] == 1
        assert got[1] == v
        assert got[2] == "x"

    async def test_vector_nested_in_map_bin(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([1.5, -2.5], VectorElementType.FLOAT64)

        await client.put(key, {"m": {"embedding": v, "count": 3}}, policy=wp)
        rec = await client.get(key)
        got = rec.bins["m"]

        assert got["embedding"] == v
        assert got["count"] == 3


class TestVectorSize:
    async def test_large_vector_round_trips(self, client_and_key):
        client, key, wp = client_and_key
        data = [i * 0.5 for i in range(4096)]
        v = Vector(data, VectorElementType.FLOAT32)

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["v"] == v

    async def test_vector_crossing_16bit_msgpack_length_boundary_in_list(self, client_and_key):
        """A vector whose wire size exceeds 65535 bytes, nested in a list bin,
        exercises the extended msgpack length header."""
        client, key, wp = client_and_key
        data = [i * 0.25 for i in range(16_384)]
        v = Vector(data, VectorElementType.FLOAT64)
        assert v.dimensions * 8 + 8 > 65_535

        await client.put(key, {"l": List([v])}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["l"][0] == v

    async def test_top_level_vector_crossing_16bit_length_boundary(self, client_and_key):
        """A top-level vector *bin* (not nested) whose particle exceeds the
        16-bit msgpack length boundary round-trips. 9000 f64 elements =>
        8 + 9000*8 = 72008 bytes, well past 65_535. Mirrors the Rust core's
        ``large_vector_crossing_16bit_length_boundary_round_trips``."""
        client, key, wp = client_and_key
        data = [i * 0.5 for i in range(9000)]
        v = Vector(data, VectorElementType.FLOAT64)
        assert v.dimensions * 8 + 8 > 65_535

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["v"] == v


# ---------------------------------------------------------------------------
# Storage permutations for the *supported* vector path.
#
# These exercise the VECTOR particle end to end through every record-level API
# that does NOT involve server-side expression evaluation. Reading a vector via
# an expression (filter/read-exp), vector-distance expressions, and Top-K
# queries are unsupported server-side today -- see vector_search_test.py,
# which keeps those documented but skipped.
# ---------------------------------------------------------------------------

# (element_type, sample values) for the three list-constructible types. FLOAT16
# has no native Python list form (numpy only), so tests that want full type
# coverage add it explicitly via ``_float16_vector``.
_STORAGE_TYPES = [
    (VectorElementType.FLOAT32, [0.5, -1.5, 2.0]),
    (VectorElementType.FLOAT64, [0.25, -0.5, 1.0]),
    (VectorElementType.INT32, [-5, 0, 7]),
]


def _float16_vector(values=(1.0, -2.5, 3.5)):
    """A FLOAT16 vector, or skip the test if numpy is unavailable."""
    np = pytest.importorskip("numpy")
    return Vector(np.array(list(values), dtype=np.float16))


@pytest_asyncio.fixture
async def vector_client(aerospike_host, use_services_alternate):
    """A bare client plus a tracked-key cleanup helper for multi-key tests."""
    cp = ClientPolicy()
    cp.use_services_alternate = use_services_alternate
    client = await new_client(cp, aerospike_host)
    created = []

    def make_key(pk):
        k = Key("test", "vector_test", pk)
        created.append(k)
        return k

    wp = WritePolicy()
    try:
        yield client, make_key, wp
    finally:
        for k in created:
            try:
                await client.delete(k, policy=wp)
            except Exception:
                pass
        await client.close()


class TestVectorViaOperate:
    """Write and read vector bins through the operate() path."""

    @pytest.mark.parametrize("element_type, values", _STORAGE_TYPES)
    async def test_operate_put_then_get_bin(self, client_and_key, element_type, values):
        client, key, wp = client_and_key
        v = Vector(values, element_type)

        await client.operate(key, [Operation.put("v", v)], policy=wp)
        rec = await client.operate(key, [Operation.get_bin("v")])

        assert rec.bins["v"] == v
        assert rec.bins["v"].element_type == element_type

    async def test_operate_put_then_get_all_bins(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([1.0, 2.0, 3.0], VectorElementType.FLOAT32)

        await client.operate(key, [Operation.put("v", v), Operation.put("n", 5)], policy=wp)
        rec = await client.operate(key, [Operation.get()])

        assert rec.bins["v"] == v
        assert rec.bins["n"] == 5

    async def test_operate_reads_multiple_vector_bins_in_one_call(self, client_and_key):
        client, key, wp = client_and_key
        a = Vector([1.0, 2.0], VectorElementType.FLOAT32)
        b = Vector([3, 4, 5], VectorElementType.INT32)

        await client.operate(key, [Operation.put("a", a), Operation.put("b", b)], policy=wp)
        rec = await client.operate(key, [Operation.get_bin("a"), Operation.get_bin("b")])

        assert rec.bins["a"] == a
        assert rec.bins["b"] == b

    async def test_operate_float16_round_trips(self, client_and_key):
        client, key, wp = client_and_key
        v = _float16_vector()

        await client.operate(key, [Operation.put("v", v)], policy=wp)
        rec = await client.operate(key, [Operation.get_bin("v")])

        assert rec.bins["v"] == v
        assert rec.bins["v"].element_type == VectorElementType.FLOAT16


class TestVectorOverwrite:
    """A vector bin can be replaced in place by writes of any shape/type."""

    async def test_overwrite_same_type_new_dimensions(self, client_and_key):
        client, key, wp = client_and_key
        await client.put(key, {"v": Vector([1.0, 2.0], VectorElementType.FLOAT32)}, policy=wp)

        new = Vector([9.0, 8.0, 7.0, 6.0], VectorElementType.FLOAT32)
        await client.put(key, {"v": new}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["v"] == new
        assert rec.bins["v"].dimensions == 4

    async def test_overwrite_changes_element_type(self, client_and_key):
        client, key, wp = client_and_key
        await client.put(key, {"v": Vector([1.0, 2.0, 3.0], VectorElementType.FLOAT32)}, policy=wp)

        new = Vector([1, 2, 3], VectorElementType.INT32)
        await client.put(key, {"v": new}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["v"] == new
        assert rec.bins["v"].element_type == VectorElementType.INT32

    async def test_overwrite_scalar_with_vector_and_back(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([1.0, 2.0], VectorElementType.FLOAT32)

        await client.put(key, {"v": 42}, policy=wp)
        assert (await client.get(key)).bins["v"] == 42

        await client.put(key, {"v": v}, policy=wp)
        assert (await client.get(key)).bins["v"] == v

        await client.put(key, {"v": "text"}, policy=wp)
        assert (await client.get(key)).bins["v"] == "text"


class TestVectorBinSelection:
    """Bin projection on reads includes/excludes vector bins as requested."""

    async def test_get_only_the_vector_bin(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([1.0, 2.0, 3.0], VectorElementType.FLOAT32)
        await client.put(key, {"v": v, "scalar": 1}, policy=wp)

        rec = await client.get(key, ["v"])

        assert rec.bins["v"] == v
        assert "scalar" not in rec.bins

    async def test_get_excluding_the_vector_bin(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([1.0, 2.0, 3.0], VectorElementType.FLOAT32)
        await client.put(key, {"v": v, "scalar": 1}, policy=wp)

        rec = await client.get(key, ["scalar"])

        assert rec.bins["scalar"] == 1
        assert "v" not in rec.bins


class TestVectorExistsTouchDelete:
    """Record-lifecycle ops behave normally for records carrying vector bins."""

    async def test_exists_touch_delete(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([1.0, 2.0], VectorElementType.FLOAT32)

        await client.put(key, {"v": v}, policy=wp)
        assert await client.exists(key) is True

        await client.touch(key)
        assert (await client.get(key)).bins["v"] == v

        await client.delete(key, policy=wp)
        assert await client.exists(key) is False

    async def test_generation_increments_on_update(self, client_and_key):
        client, key, wp = client_and_key
        await client.put(key, {"v": Vector([1.0], VectorElementType.FLOAT32)}, policy=wp)
        gen1 = (await client.get(key)).generation

        await client.put(key, {"v": Vector([2.0, 3.0], VectorElementType.FLOAT32)}, policy=wp)
        rec2 = await client.get(key)

        assert rec2.generation > gen1
        assert rec2.bins["v"] == Vector([2.0, 3.0], VectorElementType.FLOAT32)


class TestVectorBatch:
    """Batch APIs round-trip vector bins across multiple keys."""

    async def test_batch_write_then_batch_read(self, vector_client):
        client, make_key, _ = vector_client
        keys = [make_key(f"batch-{i}") for i in range(3)]
        vecs = [
            Vector([float(i), float(i) + 0.5], VectorElementType.FLOAT32) for i in range(3)
        ]
        bins_list = [{"v": vv, "i": i} for i, vv in enumerate(vecs)]

        write = await client.batch_write(
            keys, bins_list, batch_policy=BatchPolicy(), write_policy=BatchWritePolicy()
        )
        assert all(r.result_code == ResultCode.OK for r in write)

        read = await client.batch_read(keys, ["v", "i"], batch_policy=None, read_policy=None)
        assert len(read) == 3
        for i, r in enumerate(read):
            assert r.result_code == ResultCode.OK
            assert r.record.bins["v"] == vecs[i]

    async def test_batch_operate_put_and_read_back(self, vector_client):
        client, make_key, _ = vector_client
        keys = [make_key(f"batchop-{i}") for i in range(2)]
        vecs = [Vector([1, 2, 3], VectorElementType.INT32), Vector([4, 5, 6], VectorElementType.INT32)]

        ops_list = [[Operation.put("v", vv)] for vv in vecs]
        await client.batch_operate(keys, ops_list, batch_policy=None, write_policy=None)

        read = await client.batch_read(keys, ["v"], batch_policy=None, read_policy=None)
        for i, r in enumerate(read):
            assert r.result_code == ResultCode.OK
            assert r.record.bins["v"] == vecs[i]

    async def test_batch_exists_with_vector_records(self, vector_client):
        client, make_key, _ = vector_client
        present = make_key("batch-present")
        await client.put(present, {"v": Vector([1.0], VectorElementType.FLOAT32)})
        missing = make_key("batch-missing")
        await client.delete(missing)

        results = await client.batch_exists([present, missing], batch_policy=None, read_policy=None)
        assert results[0] is True
        assert results[1] is False


class TestVectorNestedAllTypes:
    """Every element type survives nesting in list and map bins, including
    deeper nesting (map-in-list, list-in-map)."""

    @pytest.mark.parametrize("element_type, values", _STORAGE_TYPES)
    async def test_each_type_nested_in_list(self, client_and_key, element_type, values):
        client, key, wp = client_and_key
        v = Vector(values, element_type)

        await client.put(key, {"l": List(["head", v, 0])}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["l"][1] == v

    @pytest.mark.parametrize("element_type, values", _STORAGE_TYPES)
    async def test_each_type_nested_in_map(self, client_and_key, element_type, values):
        client, key, wp = client_and_key
        v = Vector(values, element_type)

        await client.put(key, {"m": {"embedding": v, "n": 1}}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["m"]["embedding"] == v

    async def test_vector_in_map_in_list(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([1.0, 2.0, 3.0], VectorElementType.FLOAT32)

        await client.put(key, {"l": List([{"embedding": v}])}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["l"][0]["embedding"] == v

    async def test_vector_in_list_in_map(self, client_and_key):
        client, key, wp = client_and_key
        v = Vector([1, 2, 3], VectorElementType.INT32)

        await client.put(key, {"m": {"items": List([v])}}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["m"]["items"][0] == v


class TestVectorAllElementTypesInOneRecord:
    async def test_all_four_element_types_in_one_record(self, client_and_key):
        """A record with a separate bin per element type (float16 needs numpy)
        round-trips every bin independently."""
        np = pytest.importorskip("numpy")
        client, key, wp = client_and_key

        bins = {
            "f16": Vector(np.array([1.0, -2.5, 3.5], dtype=np.float16)),
            "f32": Vector([0.5, -1.5, 2.0], VectorElementType.FLOAT32),
            "f64": Vector([0.25, -0.5, 1.0], VectorElementType.FLOAT64),
            "i32": Vector([-5, 0, 7], VectorElementType.INT32),
        }
        await client.put(key, bins, policy=wp)
        rec = await client.get(key)

        for name, expected in bins.items():
            assert rec.bins[name] == expected, name
            assert rec.bins[name].element_type == expected.element_type, name


class TestVectorElementTypeFidelity:
    """Element-type and dimension edge cases on the round trip."""

    @pytest.mark.parametrize("element_type, values", _STORAGE_TYPES)
    async def test_single_dimension_vector_round_trips(self, client_and_key, element_type, values):
        """A one-element vector (header + a single element) round-trips for
        every list-constructible element type."""
        client, key, wp = client_and_key
        v = Vector([values[0]], element_type)
        assert v.dimensions == 1

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["v"] == v
        assert rec.bins["v"].dimensions == 1
        assert rec.bins["v"].element_type == element_type

    async def test_single_dimension_float16_round_trips(self, client_and_key):
        client, key, wp = client_and_key
        v = _float16_vector((1.0,))
        assert v.dimensions == 1

        await client.put(key, {"v": v}, policy=wp)
        rec = await client.get(key)

        assert rec.bins["v"] == v
        assert rec.bins["v"].element_type == VectorElementType.FLOAT16

    async def test_same_value_preserves_element_type_distinctly(self, client_and_key):
        """The literal value "one" stored as four element types is kept
        distinct by the server -- the element-type byte is not coalesced, and
        two bins holding the same number but different element types compare
        unequal after the round trip. Mirrors the Rust core's
        ``element_type_is_preserved_distinctly_through_the_server``."""
        np = pytest.importorskip("numpy")
        client, key, wp = client_and_key

        bins = {
            "f16": Vector(np.array([1.0], dtype=np.float16)),
            "i32": Vector([1], VectorElementType.INT32),
            "f32": Vector([1.0], VectorElementType.FLOAT32),
            "f64": Vector([1.0], VectorElementType.FLOAT64),
        }
        await client.put(key, bins, policy=wp)
        rec = await client.get(key)

        assert rec.bins["f16"].element_type == VectorElementType.FLOAT16
        assert rec.bins["i32"].element_type == VectorElementType.INT32
        assert rec.bins["f32"].element_type == VectorElementType.FLOAT32
        assert rec.bins["f64"].element_type == VectorElementType.FLOAT64

        # Same number, different element type => not equal after the round trip.
        assert rec.bins["f32"] != rec.bins["f64"]
        assert rec.bins["f32"] != rec.bins["i32"]

    async def test_signed_zero_stays_distinct_from_positive_zero(self, client_and_key):
        """-0.0 must not be flattened to +0.0 by the round trip (guards the
        IEEE-754 bit pattern, matching the Rust core's signed-zero test)."""
        np = pytest.importorskip("numpy")
        client, key, wp = client_and_key

        neg = Vector(np.array([-0.0], dtype=np.float32))
        await client.put(key, {"v": neg}, policy=wp)
        got = (await client.get(key)).bins["v"].numpy_value

        assert got.tobytes() == np.array([-0.0], dtype=np.float32).tobytes()
        assert got.tobytes() != np.array([0.0], dtype=np.float32).tobytes()
