"""Unit tests for pipeline/features/versioning.py"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from pipeline.features.versioning import get_feature_version, assert_version_compatible
from pipeline.exceptions import FeatureVersionMismatchError


_CONFIG = {
    "rolling_windows": [3, 5, 10],
    "min_observations": 3,
    "scoring": {"pts": 1.0, "reb": 1.2, "ast": 1.5},
}


def test_same_config_produces_same_version():
    v1 = get_feature_version(_CONFIG)
    v2 = get_feature_version(_CONFIG)
    assert v1 == v2


def test_different_config_produces_different_version():
    modified = {**_CONFIG, "min_observations": 5}
    assert get_feature_version(_CONFIG) != get_feature_version(modified)


def test_version_is_12_hex_chars():
    v = get_feature_version(_CONFIG)
    assert len(v) == 12
    assert all(c in "0123456789abcdef" for c in v)


def test_key_order_does_not_affect_version():
    config_a = {"a": 1, "b": 2}
    config_b = {"b": 2, "a": 1}
    assert get_feature_version(config_a) == get_feature_version(config_b)


def test_changing_rolling_windows_changes_version():
    v1 = get_feature_version({**_CONFIG, "rolling_windows": [3, 5]})
    v2 = get_feature_version({**_CONFIG, "rolling_windows": [3, 5, 10]})
    assert v1 != v2


def test_assert_version_compatible_passes_when_same():
    v = get_feature_version(_CONFIG)
    assert_version_compatible(v, v)  # should not raise


def test_assert_version_compatible_raises_when_different():
    v1 = get_feature_version(_CONFIG)
    v2 = get_feature_version({**_CONFIG, "min_observations": 99})
    with pytest.raises(FeatureVersionMismatchError):
        assert_version_compatible(v1, v2)


def test_empty_config_produces_valid_version():
    v = get_feature_version({})
    assert len(v) == 12
