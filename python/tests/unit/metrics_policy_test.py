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

from aerospike_async import (
    CommandType,
    HistogramType,
    LatencyUnit,
    MetricsPolicy,
    Sampler,
)


def test_default_policy_is_micros():
    """Default policy records microseconds with 24 logarithmic columns."""
    mp = MetricsPolicy()
    assert mp.latency_unit == LatencyUnit.MICROSECONDS
    assert mp.latency_columns == 24
    assert mp.latency_base == 2
    assert mp.histogram_type == HistogramType.LOGARITHMIC
    assert mp.sampler == Sampler.all()
    assert mp.labels == []


def test_micros_preset_matches_default():
    mp = MetricsPolicy.micros()
    default = MetricsPolicy()
    assert mp.latency_unit == default.latency_unit
    assert mp.latency_columns == default.latency_columns
    assert mp.latency_base == default.latency_base


def test_millis_preset():
    """Millis preset selects the classic milliseconds/7-column scheme."""
    mp = MetricsPolicy.millis()
    assert mp.latency_unit == LatencyUnit.MILLISECONDS
    assert mp.latency_columns == 7
    assert mp.latency_base == 2


def test_policy_properties_round_trip():
    mp = MetricsPolicy()
    mp.latency_unit = LatencyUnit.MILLISECONDS
    mp.latency_columns = 9
    mp.latency_base = 3
    mp.histogram_type = HistogramType.LINEAR
    mp.sampler = Sampler.probability(0.25)
    mp.labels = [{"team": "billing", "region": "us-west"}]

    assert mp.latency_unit == LatencyUnit.MILLISECONDS
    assert mp.latency_columns == 9
    assert mp.latency_base == 3
    assert mp.histogram_type == HistogramType.LINEAR
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
    assert "latency_unit=us" in text
    assert "latency_columns=24" in text
