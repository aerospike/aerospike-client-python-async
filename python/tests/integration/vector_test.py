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

from aerospike_async import ClientPolicy, Key, List, Vector, VectorElementType, WritePolicy, new_client


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
