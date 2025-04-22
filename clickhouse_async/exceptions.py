"""Exceptions for ClickHouse client."""


class ClickHouseError(Exception):
    """Base class for all ClickHouse exceptions."""

    pass


class ProtocolError(ClickHouseError):
    """Protocol-related errors."""

    pass


class RemoteConnectionError(ClickHouseError):
    """Connection-related errors."""

    pass


class RemoteServerError(ClickHouseError):
    """Server-side errors."""

    def __init__(
        self,
        code: int,
        name: str,
        message: str,
        stack_trace: str,
        nested: "RemoteServerError | None" = None,
    ) -> None:
        """Initialize server exception.

        Args:
            code: Error code
            name: Error name
            message: Error message
            stack_trace: Stack trace
            nested: Nested exception
        """
        self.code = code
        self.name = name
        self.message = message
        self.stack_trace = stack_trace
        self.nested = nested
        super().__init__(f"{name} ({code}): {message}")
