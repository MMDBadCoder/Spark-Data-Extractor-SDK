class ExtractionError(Exception):
    """A customer-facing error with a stable machine-readable code."""

    def __init__(self, code, message=None):
        from .messages import message as catalog_message
        self.code = code
        self.message = message if message is not None else catalog_message(code)
        super().__init__(f"[{code}] {self.message}")
