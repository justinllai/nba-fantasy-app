"""
Feature schema versioning for the NBA stats data pipeline.
Produces a deterministic version string from a features_config dict.
"""
import hashlib
import json

from pipeline.exceptions import FeatureVersionMismatchError


def get_feature_version(features_config: dict) -> str:
    """
    Return a 12-char hex version string derived from features_config.

    The hash is deterministic: same config always produces same version,
    regardless of key insertion order. Any config change produces a different version.

    Args:
        features_config: Feature engineering configuration dict

    Returns:
        12 lowercase hex characters (first 12 of SHA-256 hash)
    """
    serialized = json.dumps(features_config, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return digest[:12]


def assert_version_compatible(stored_version: str, current_version: str) -> None:
    """
    Raise FeatureVersionMismatchError if stored and current versions differ.

    Args:
        stored_version: Version hash from the previously written feature sidecar
        current_version: Version hash from the current features_config

    Raises:
        FeatureVersionMismatchError: If versions do not match
    """
    if stored_version != current_version:
        raise FeatureVersionMismatchError(
            f"Feature schema version mismatch: stored={stored_version!r}, "
            f"current={current_version!r}. "
            "Re-run feature engineering with the current config to regenerate."
        )
