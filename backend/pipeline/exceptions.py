"""
Typed exceptions for the NBA stats data pipeline.
Every failure path in the spec maps to one of these classes.
"""


class PipelineConfigError(Exception):
    """Raised before any I/O — bad arguments or pipeline misconfiguration."""

    def __init__(self, message="Pipeline configuration error. Check arguments."):
        super().__init__(message)


class APIKeyMissingError(PipelineConfigError):
    """Raised when BALL_IS_LIFE environment variable is not set."""

    def __init__(self, message="BALL_IS_LIFE API key not found in environment. Set BALL_IS_LIFE in your .env file."):
        super().__init__(message)


class UnsupportedDataTypeError(PipelineConfigError):
    """Raised when a data type is not in SUPPORTED_DATA_TYPES."""

    def __init__(self, data_type: str = None, message: str = None):
        if message is None:
            message = f"Unsupported data type: '{data_type}'. Must be one of the supported data types."
        super().__init__(message)


class APIFetchError(Exception):
    """Raised when API calls fail after all retries (timeout, 429, mid-stream failure)."""

    def __init__(self, message="API fetch failed. Check logs for details."):
        super().__init__(message)


class ValidationError(Exception):
    """Raised when required structural fields fail validation. Halts the affected data type."""

    def __init__(self, message="Data validation failed. Check logs for details."):
        super().__init__(message)


class IngestionError(Exception):
    """Raised when data ingestion fails after all retries are exhausted."""

    def __init__(self, message="Data ingestion failed. Check logs for details."):
        super().__init__(message)


class FileWriteError(Exception):
    """Raised when a parquet or sidecar write operation fails."""

    def __init__(self, message="File write operation failed. Check logs for details."):
        super().__init__(message)


class EntityLookupError(Exception):
    """Raised when a player or team entity cannot be found in the API."""

    def __init__(self, entity: str = None, message: str = None):
        if message is None:
            message = f"Entity not found: '{entity}'. Check player name or team abbreviation."
        super().__init__(message)


# Backward-compatible aliases (used by original run.py — remove after T053 rewrite)
MissingAPIKeyError = APIKeyMissingError


class FeatureVersionMismatchError(Exception):
    """Raised when a stored model's feature schema version does not match the current feature output."""

    def __init__(self, stored: str = None, current: str = None, message: str = None):
        if message is None:
            message = (
                f"Feature schema version mismatch: stored='{stored}', current='{current}'. "
                "Retrain model or regenerate features with the stored config."
            )
        super().__init__(message)
