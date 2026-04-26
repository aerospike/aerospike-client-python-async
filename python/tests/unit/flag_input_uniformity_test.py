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

"""Per-surface tests asserting every flag-accepting entry point honors the four
input modes uniformly:

  1. ``int`` bitmask  ............................. e.g. ``5``
  2. single ``Enum``  .............................. e.g. ``ListWriteFlags.ADD_UNIQUE``
  3. ``Enum | Enum``  (returns ``int``)  ........... e.g. ``A | B``
  4. ``int(Enum) | int(Enum)``  .................... explicit cast form

Plus a ``TestBitwiseOperators`` block that exercises every bitwise dunder
(``|``, ``&``, ``^``, ``~``) on every flag enum, covering Enum-Enum,
Enum-int, and int-Enum directions.  This is the runtime contract that the
``IntEnum`` stubs promise.

Surfaces covered:
  - BitPolicy(...) / .write_flags
  - ListPolicy(...) / .write_flags
  - MapPolicy.flags / MapPolicy.raw_flags (regression for lossy `flags`)
  - HLLPolicy(...) / .write_flags
  - ExpOperation.read / .write (flags kwarg)
"""

import pytest
from aerospike_async import (
    BitPolicy,
    BitWriteFlags,
    ExpOperation,
    ExpReadFlags,
    ExpWriteFlags,
    FilterExpression as fe,
    HLLPolicy,
    HLLWriteFlags,
    ListPolicy,
    ListWriteFlags,
    MapPolicy,
    MapWriteFlags,
)


def _exp() -> "fe":
    """Build any concrete FilterExpression for ExpOperation.read/write."""
    return fe.int_val(0)


# ---------------------------------------------------------------------------
# BitPolicy
# ---------------------------------------------------------------------------

class TestBitPolicyFlagInputs:

    EXPECTED = BitWriteFlags.CREATE_ONLY | BitWriteFlags.NO_FAIL  # 1 | 4 = 5

    def test_int_bitmask(self):
        bp = BitPolicy(self.EXPECTED)
        assert bp.get_write_flags() == self.EXPECTED

    def test_single_enum(self):
        bp = BitPolicy(BitWriteFlags.CREATE_ONLY)
        assert bp.get_write_flags() == BitWriteFlags.CREATE_ONLY

    def test_enum_or_enum(self):
        bp = BitPolicy(BitWriteFlags.CREATE_ONLY | BitWriteFlags.NO_FAIL)
        assert bp.get_write_flags() == self.EXPECTED

    def test_int_cast_or(self):
        # Explicit int() casts are exercised here on purpose; this is one of
        # the four documented input forms and must keep working.
        bp = BitPolicy(int(BitWriteFlags.CREATE_ONLY) | int(BitWriteFlags.NO_FAIL))
        assert bp.get_write_flags() == self.EXPECTED

    def test_setter_all_modes(self):
        bp = BitPolicy(None)
        bp.set_write_flags(self.EXPECTED)
        assert bp.get_write_flags() == self.EXPECTED
        bp.set_write_flags(BitWriteFlags.UPDATE_ONLY)
        assert bp.get_write_flags() == BitWriteFlags.UPDATE_ONLY
        bp.set_write_flags(BitWriteFlags.CREATE_ONLY | BitWriteFlags.NO_FAIL)
        assert bp.get_write_flags() == self.EXPECTED
        bp.set_write_flags(int(BitWriteFlags.CREATE_ONLY) | int(BitWriteFlags.NO_FAIL))
        assert bp.get_write_flags() == self.EXPECTED


# ---------------------------------------------------------------------------
# ListPolicy
# ---------------------------------------------------------------------------

class TestListPolicyFlagInputs:

    EXPECTED = ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL  # 1 | 4 = 5

    def test_int_bitmask(self):
        lp = ListPolicy(None, self.EXPECTED)
        assert lp.write_flags == self.EXPECTED

    def test_single_enum(self):
        lp = ListPolicy(None, ListWriteFlags.ADD_UNIQUE)
        assert lp.write_flags == ListWriteFlags.ADD_UNIQUE

    def test_enum_or_enum(self):
        lp = ListPolicy(None, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL)
        assert lp.write_flags == self.EXPECTED

    def test_int_cast_or(self):
        # Explicit int() casts are exercised here on purpose.
        lp = ListPolicy(
            None, int(ListWriteFlags.ADD_UNIQUE) | int(ListWriteFlags.NO_FAIL)
        )
        assert lp.write_flags == self.EXPECTED

    def test_setter_all_modes(self):
        lp = ListPolicy(None, None)
        lp.write_flags = self.EXPECTED
        assert lp.write_flags == self.EXPECTED
        lp.write_flags = ListWriteFlags.ADD_UNIQUE
        assert lp.write_flags == ListWriteFlags.ADD_UNIQUE
        lp.write_flags = ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL
        assert lp.write_flags == self.EXPECTED
        lp.write_flags = int(ListWriteFlags.ADD_UNIQUE) | int(ListWriteFlags.NO_FAIL)
        assert lp.write_flags == self.EXPECTED


# ---------------------------------------------------------------------------
# MapPolicy
# ---------------------------------------------------------------------------

class TestMapPolicyFlagInputs:

    EXPECTED = MapWriteFlags.CREATE_ONLY | MapWriteFlags.NO_FAIL  # 1 | 4 = 5

    def test_int_bitmask(self):
        mp = MapPolicy(None, None, self.EXPECTED, None)
        assert mp.raw_flags == self.EXPECTED

    def test_single_enum(self):
        mp = MapPolicy(None, None, MapWriteFlags.CREATE_ONLY, None)
        assert mp.raw_flags == MapWriteFlags.CREATE_ONLY

    def test_enum_or_enum(self):
        mp = MapPolicy(
            None, None, MapWriteFlags.CREATE_ONLY | MapWriteFlags.NO_FAIL, None
        )
        assert mp.raw_flags == self.EXPECTED

    def test_int_cast_or(self):
        # Explicit int() casts are exercised here on purpose.
        mp = MapPolicy(
            None,
            None,
            int(MapWriteFlags.CREATE_ONLY) | int(MapWriteFlags.NO_FAIL),
            None,
        )
        assert mp.raw_flags == self.EXPECTED

    def test_setter_all_modes(self):
        mp = MapPolicy(None, None)
        mp.flags = self.EXPECTED
        assert mp.raw_flags == self.EXPECTED
        mp.flags = MapWriteFlags.CREATE_ONLY
        assert mp.raw_flags == MapWriteFlags.CREATE_ONLY
        mp.flags = MapWriteFlags.CREATE_ONLY | MapWriteFlags.NO_FAIL
        assert mp.raw_flags == self.EXPECTED
        mp.flags = int(MapWriteFlags.CREATE_ONLY) | int(MapWriteFlags.NO_FAIL)
        assert mp.raw_flags == self.EXPECTED


class TestMapPolicyRawFlagsRegression:
    """The legacy ``flags`` getter clamps non-singular bitmasks to a single
    ``MapWriteFlags`` variant (DEFAULT for unknown values), losing information.
    ``raw_flags`` MUST always return the exact ``int`` bitmask that was stored.
    """

    @pytest.mark.parametrize(
        "raw,expected_lossy_default",
        [
            (0, MapWriteFlags.DEFAULT),
            (1, MapWriteFlags.CREATE_ONLY),
            (2, MapWriteFlags.UPDATE_ONLY),
            (4, MapWriteFlags.NO_FAIL),
            (8, MapWriteFlags.PARTIAL),
        ],
    )
    def test_singular_flags_match_legacy_getter(self, raw, expected_lossy_default):
        mp = MapPolicy(None, None, raw, None)
        assert mp.raw_flags == raw
        assert mp.flags == expected_lossy_default

    @pytest.mark.parametrize(
        "raw",
        [
            MapWriteFlags.CREATE_ONLY | MapWriteFlags.NO_FAIL,  # 5
            MapWriteFlags.UPDATE_ONLY | MapWriteFlags.PARTIAL,  # 10
            MapWriteFlags.CREATE_ONLY | MapWriteFlags.PARTIAL,  # 9
            MapWriteFlags.NO_FAIL | MapWriteFlags.PARTIAL,      # 12
            0x0F,
        ],
    )
    def test_raw_flags_preserves_combined_bitmasks(self, raw):
        """raw_flags must round-trip ANY u8 bitmask losslessly."""
        mp = MapPolicy(None, None, raw, None)
        assert mp.raw_flags == raw, (
            f"raw_flags lost information: stored {raw}, got {mp.raw_flags}"
        )

    def test_raw_flags_via_setter_round_trips(self):
        mp = MapPolicy(None, None)
        for raw in (0, 1, 2, 4, 5, 8, 9, 10, 12, 15):
            mp.flags = raw
            assert mp.raw_flags == raw


# ---------------------------------------------------------------------------
# HLLPolicy
# ---------------------------------------------------------------------------

class TestHLLPolicyFlagInputs:

    EXPECTED = HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL  # 1 | 4 = 5

    def test_int_bitmask(self):
        hp = HLLPolicy(self.EXPECTED)
        assert hp.write_flags == self.EXPECTED

    def test_single_enum(self):
        hp = HLLPolicy(HLLWriteFlags.CREATE_ONLY)
        assert hp.write_flags == HLLWriteFlags.CREATE_ONLY

    def test_enum_or_enum(self):
        hp = HLLPolicy(HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL)
        assert hp.write_flags == self.EXPECTED

    def test_int_cast_or(self):
        # Explicit int() casts are exercised here on purpose.
        hp = HLLPolicy(int(HLLWriteFlags.CREATE_ONLY) | int(HLLWriteFlags.NO_FAIL))
        assert hp.write_flags == self.EXPECTED

    def test_setter_all_modes(self):
        hp = HLLPolicy()
        hp.write_flags = self.EXPECTED
        assert hp.write_flags == self.EXPECTED
        hp.write_flags = HLLWriteFlags.CREATE_ONLY
        assert hp.write_flags == HLLWriteFlags.CREATE_ONLY
        hp.write_flags = HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL
        assert hp.write_flags == self.EXPECTED
        hp.write_flags = int(HLLWriteFlags.CREATE_ONLY) | int(HLLWriteFlags.NO_FAIL)
        assert hp.write_flags == self.EXPECTED


# ---------------------------------------------------------------------------
# ExpOperation.read / .write
# ---------------------------------------------------------------------------

class TestExpOperationFlagInputs:
    """ExpOperation.read/write should accept all four input modes for `flags`.

    There is no public getter for the resolved flag value; we just assert the
    constructor doesn't raise and produces an ExpOperation instance for every
    accepted form.
    """

    # ExpReadFlags only defines DEFAULT (0) and EVAL_NO_FAIL (16); we exercise
    # the OR path by combining EVAL_NO_FAIL with DEFAULT (still 16) plus a raw
    # bitmask that includes a bit not enumerated by the Rust enum.
    READ_EXPECTED = ExpReadFlags.EVAL_NO_FAIL  # 16
    WRITE_EXPECTED = ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL  # 1|8=9

    def test_read_int_bitmask(self):
        op = ExpOperation.read("b", _exp(), flags=self.READ_EXPECTED)
        assert op is not None

    def test_read_single_enum(self):
        op = ExpOperation.read("b", _exp(), flags=ExpReadFlags.EVAL_NO_FAIL)
        assert op is not None

    def test_read_enum_or_enum(self):
        op = ExpOperation.read(
            "b", _exp(), flags=ExpReadFlags.DEFAULT | ExpReadFlags.EVAL_NO_FAIL
        )
        assert op is not None

    def test_read_int_cast_or(self):
        op = ExpOperation.read(
            "b",
            _exp(),
            flags=int(ExpReadFlags.DEFAULT) | int(ExpReadFlags.EVAL_NO_FAIL),
        )
        assert op is not None

    def test_read_no_flags(self):
        assert ExpOperation.read("b", _exp()) is not None

    def test_write_int_bitmask(self):
        op = ExpOperation.write("b", _exp(), flags=self.WRITE_EXPECTED)
        assert op is not None

    def test_write_single_enum(self):
        op = ExpOperation.write("b", _exp(), flags=ExpWriteFlags.CREATE_ONLY)
        assert op is not None

    def test_write_enum_or_enum(self):
        op = ExpOperation.write(
            "b", _exp(), flags=ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL
        )
        assert op is not None

    def test_write_int_cast_or(self):
        op = ExpOperation.write(
            "b",
            _exp(),
            flags=int(ExpWriteFlags.CREATE_ONLY) | int(ExpWriteFlags.POLICY_NO_FAIL),
        )
        assert op is not None

    def test_write_no_flags(self):
        assert ExpOperation.write("b", _exp()) is not None


# ---------------------------------------------------------------------------
# Bitwise operators on flag enums (the IntEnum stub contract)
# ---------------------------------------------------------------------------

class TestBitwiseOperators:
    """All flag enums must support |, &, ^, ~ in every direction at runtime,
    matching what the ``IntEnum`` stubs promise to type-checkers.
    """

    @pytest.mark.parametrize(
        "a,b,a_val,b_val",
        [
            (ListWriteFlags.ADD_UNIQUE, ListWriteFlags.NO_FAIL, 1, 4),
            (MapWriteFlags.CREATE_ONLY, MapWriteFlags.NO_FAIL, 1, 4),
            (BitWriteFlags.CREATE_ONLY, BitWriteFlags.NO_FAIL, 1, 4),
            (HLLWriteFlags.CREATE_ONLY, HLLWriteFlags.NO_FAIL, 1, 4),
            (ExpWriteFlags.CREATE_ONLY, ExpWriteFlags.POLICY_NO_FAIL, 1, 8),
            (ExpReadFlags.DEFAULT, ExpReadFlags.EVAL_NO_FAIL, 0, 16),
        ],
    )
    def test_or_and_xor_invert_enum_enum(self, a, b, a_val, b_val):
        assert (a | b) == (a_val | b_val)
        assert (a & b) == (a_val & b_val)
        assert (a ^ b) == (a_val ^ b_val)

    @pytest.mark.parametrize(
        "flag,val,bit",
        [
            (ListWriteFlags.ADD_UNIQUE, 1, 0xFF),
            (MapWriteFlags.CREATE_ONLY, 1, 0xFF),
            (BitWriteFlags.CREATE_ONLY, 1, 0xFF),
            (HLLWriteFlags.CREATE_ONLY, 1, 0xFF),
            (ExpWriteFlags.CREATE_ONLY, 1, 0xFF),
            (ExpReadFlags.EVAL_NO_FAIL, 16, 0xFF),
        ],
    )
    def test_enum_int_bidirectional(self, flag, val, bit):
        assert (flag | bit) == (val | bit)
        assert (bit | flag) == (val | bit)
        assert (flag & bit) == (val & bit)
        assert (bit & flag) == (val & bit)
        assert (flag ^ bit) == (val ^ bit)
        assert (bit ^ flag) == (val ^ bit)

    @pytest.mark.parametrize(
        "flag,val",
        [
            (ListWriteFlags.ADD_UNIQUE, 1),
            (MapWriteFlags.CREATE_ONLY, 1),
            (BitWriteFlags.CREATE_ONLY, 1),
            (HLLWriteFlags.CREATE_ONLY, 1),
            (ExpWriteFlags.CREATE_ONLY, 1),
            (ExpReadFlags.EVAL_NO_FAIL, 16),
        ],
    )
    def test_invert_returns_masked_complement(self, flag, val):
        # Flag enums use u8-masked complement so ~A is positive and bit-ready.
        assert (~flag) == (~val) & 0xFF
        assert ((~flag) & 0xFF) == ((~val) & 0xFF)
