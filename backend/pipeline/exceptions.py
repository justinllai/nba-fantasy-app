"""
Custom exceptions for the NBA stats data pipeline.
"""


class MissingAPIKeyError(Exception):
    """Raised when BALL_IS_LIFE environment variable is not set."""

    def __init__(self, message="BALL_IS_LIFE API key not found in environment. Set BALL_IS_LIFE in your .env file."):
        super().__init__(message)


class UnsupportedDataTypeError(Exception):
    """Raised when a data type is not in SUPPORTED_DATA_TYPES."""

    def __init__(self, data_type: str = None, message: str = None):
        if message is None:
            message = f"Unsupported data type: '{data_type}'. Must be one of the supported data types."
        super().__init__(message)


class ValidationError(Exception):
    """Raised when DataFrame validation fails (missing columns, wrong dtypes, insufficient rows)."""

    def __init__(self, message="Data validation failed. Check logs for details."):
        super().__init__(message)


class IngestionError(Exception):
    """Raised when data ingestion fails after all retries are exhausted."""

    def __init__(self, message="Data ingestion failed. Check logs for details."):
        super().__init__(message)


class FileWriteError(Exception):
    """Raised when a file write operation fails."""

    def __init__(self, message="File write operation failed. Check logs for details."):
        super().__init__(message)


class EntityLookupError(Exception):
    """Raised when a player or team entity cannot be found."""

    def __init__(self, entity: str = None, message: str = None):
        if message is None:
            message = f"Entity not found: '{entity}'. Check player name or team abbreviation."
        super().__init__(message)
