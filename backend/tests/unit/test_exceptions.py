"""Unit tests for pipeline/exceptions.py"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from pipeline.exceptions import (
    PipelineConfigError,
    APIKeyMissingError,
    UnsupportedDataTypeError,
    APIFetchError,
    ValidationError,
    IngestionError,
    FileWriteError,
    EntityLookupError,
    FeatureVersionMismatchError,
)


def test_all_exceptions_importable():
    # Just importing above is sufficient — this test confirms they're all present
    assert PipelineConfigError
    assert APIKeyMissingError
    assert UnsupportedDataTypeError
    assert APIFetchError
    assert ValidationError
    assert IngestionError
    assert FileWriteError
    assert EntityLookupError
    assert FeatureVersionMismatchError


def test_api_key_missing_error_is_pipeline_config_error():
    assert issubclass(APIKeyMissingError, PipelineConfigError)


def test_unsupported_data_type_error_is_pipeline_config_error():
    assert issubclass(UnsupportedDataTypeError, PipelineConfigError)


def test_pipeline_config_error_raises():
    with pytest.raises(PipelineConfigError):
        raise PipelineConfigError("bad config")


def test_api_key_missing_error_raises_and_has_default_message():
    with pytest.raises(APIKeyMissingError) as exc_info:
        raise APIKeyMissingError()
    assert "BALL_IS_LIFE" in str(exc_info.value)


def test_unsupported_data_type_error_includes_type_in_message():
    with pytest.raises(UnsupportedDataTypeError) as exc_info:
        raise UnsupportedDataTypeError("bad_type")
    assert "bad_type" in str(exc_info.value)


def test_api_fetch_error_raises():
    with pytest.raises(APIFetchError):
        raise APIFetchError("timeout on chunk 3")


def test_validation_error_raises():
    with pytest.raises(ValidationError):
        raise ValidationError("missing column: pts")


def test_ingestion_error_raises():
    with pytest.raises(IngestionError):
        raise IngestionError()


def test_file_write_error_raises():
    with pytest.raises(FileWriteError):
        raise FileWriteError("failed to write parquet")


def test_entity_lookup_error_includes_entity_in_message():
    with pytest.raises(EntityLookupError) as exc_info:
        raise EntityLookupError("LeBron James")
    assert "LeBron James" in str(exc_info.value)


def test_feature_version_mismatch_error_includes_versions():
    with pytest.raises(FeatureVersionMismatchError) as exc_info:
        raise FeatureVersionMismatchError(stored="abc123", current="def456")
    msg = str(exc_info.value)
    assert "abc123" in msg
    assert "def456" in msg


def test_all_exceptions_are_exception_subclasses():
    for cls in [
        PipelineConfigError, APIKeyMissingError, UnsupportedDataTypeError,
        APIFetchError, ValidationError, IngestionError,
        FileWriteError, EntityLookupError, FeatureVersionMismatchError,
    ]:
        assert issubclass(cls, Exception), f"{cls.__name__} should subclass Exception"
