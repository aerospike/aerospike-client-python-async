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

import pytest

from aerospike_async import (
    CommandType,
    LatencyUnit,
    MetricsPolicy,
    Sampler,
)


def test_default_policy_is_the_cross_sdk_default():
    """Milliseconds / 7 columns / shift 1, not the core's microsecond preset.

    Deliberately not inherited from the core: a binding whose default differed
    from every other client would mislead anyone reading one shared config.
    """
    mp = MetricsPolicy()
    assert mp.latency_unit == LatencyUnit.MILLISECONDS
    assert mp.latency_columns == 7
    assert mp.latency_shift == 1
    assert mp.sampler == Sampler.all()
    assert mp.labels == []


def test_micros_preset_is_the_microsecond_scheme():
    mp = MetricsPolicy.micros()
    assert mp.latency_unit == LatencyUnit.MICROSECONDS
    assert mp.latency_columns == 24


def test_millis_preset():
    """Millis preset selects the classic milliseconds/7-column scheme."""
    mp = MetricsPolicy.millis()
    assert mp.latency_unit == LatencyUnit.MILLISECONDS
    assert mp.latency_columns == 7
    assert mp.latency_shift == 1


def test_latency_base_is_derived_from_the_shift():
    """One knob, not two: base is always 2 ** shift and cannot be set apart."""
    mp = MetricsPolicy()
    mp.latency_shift = 3
    assert mp.latency_base == 8
    with pytest.raises(AttributeError):
        mp.latency_base = 99


def test_latency_shift_below_one_is_rejected():
    """shift=0 means a multiplier of 1, where every boundary is the same."""
    mp = MetricsPolicy()
    with pytest.raises(ValueError, match="at least 1"):
        mp.latency_shift = 0


def test_histogram_type_is_not_exposed():
    """Logarithmic is the only layout the SDKs offer, so the knob is absent."""
    assert not hasattr(MetricsPolicy(), "histogram_type")


def test_policy_properties_round_trip():
    mp = MetricsPolicy()
    mp.latency_unit = LatencyUnit.MILLISECONDS
    mp.latency_columns = 9
    mp.latency_shift = 3
    mp.sampler = Sampler.probability(0.25)
    mp.labels = [{"team": "billing", "region": "us-west"}]

    assert mp.latency_unit == LatencyUnit.MILLISECONDS
    assert mp.latency_columns == 9
    assert mp.latency_shift == 3
    assert mp.sampler.range == 1_000_000
    assert mp.sampler.threshold == 250_000
    assert mp.labels == [{"team": "billing", "region": "us-west"}]


def test_policy_drops_empty_label_maps():
    mp = MetricsPolicy()
    mp.labels = [{}, {"env": "prod"}, {}]
    assert mp.labels == [{"env": "prod"}]


def test_sampler_constructors():
    assert Sampler.all().range == 1
    assert Sampler.all().threshold == 1

    assert Sampler.never().range == 0
    assert Sampler.never().threshold == 0

    half = Sampler.probability(0.5)
    assert half.range == 1_000_000
    assert half.threshold == 500_000

    # Probability is clamped to [0, 1].
    assert Sampler.probability(2.0).threshold == 1_000_000
    assert Sampler.probability(-1.0).threshold == 0

    explicit = Sampler(10, 3)
    assert explicit.range == 10
    assert explicit.threshold == 3

    # Threshold is clamped to range; range is forced to at least 1.
    clamped = Sampler(5, 50)
    assert clamped.threshold == 5
    assert Sampler(0, 0).range == 1


def test_latency_unit_str_is_wire_form():
    assert str(LatencyUnit.MICROSECONDS) == "us"
    assert str(LatencyUnit.MILLISECONDS) == "ms"


def test_command_type_str_matches_serialized_keys():
    assert str(CommandType.GET_HEADER) == "GetHeader"
    assert str(CommandType.UDF) == "UDF"
    assert str(CommandType.BATCH_WRITE) == "BatchWrite"


def test_policy_repr_names_the_essentials():
    text = repr(MetricsPolicy())
    assert "latency_unit=ms" in text
    assert "latency_columns=7" in text
    assert "latency_shift=1" in text
