"""Gemini API client with normalized error classification.

Owns:
- HTTP invocation to Gemini API
- Response parsing
- Error classification (retryable vs permanent)
- Retry-After extraction from 429 responses

Does NOT own:
- Rate limiting (scheduler)
- Key rotation (scheduler)
- Retry logic (scheduler)
- Prompt building (gemini_processor)
- Output validation (gemini_processor)
"""

import json
import urllib.request
import urllib.error
import ssl
from datetime import datetime, timezone
from enum import Enum

try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()

API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
DEFAULT_MODEL = "gemini-3.1-flash-lite"
REQUEST_TIMEOUT = 60


class ErrorClass(Enum):
    """Classification of Gemini API errors."""
    RETRYABLE = "retryable"          # 429, 5xx, timeout, transient
    PERMANENT = "permanent"          # 404, 400, auth error
    UNKNOWN = "unknown"


class GeminiError(Exception):
    """Classified error from a Gemini API call."""

    def __init__(self, error_class, status_code=None, message="",
                 retry_after=None):
        self.error_class = error_class
        self.status_code = status_code
        self.message = message
        self.retry_after = retry_after  # seconds, or None
        super().__init__(message)

    @property
    def retryable(self):
        return self.error_class == ErrorClass.RETRYABLE

    @property
    def permanent(self):
        return self.error_class == ErrorClass.PERMANENT

    def __repr__(self):
        return (
            f"GeminiError({self.error_class.value}, "
            f"status={self.status_code}, retry_after={self.retry_after})"
        )


def classify_error(exc):
    """Classify an exception into error categories.

    Returns:
        GeminiError with classified error type and metadata.
    """
    status_code = None
    retry_after = None
    message = str(exc)

    # Extract status code from urllib errors
    if isinstance(exc, urllib.error.HTTPError):
        status_code = exc.code
        try:
            body = exc.read().decode("utf-8", errors="replace")
            err_data = json.loads(body)
            err_detail = err_data.get("error", {})
            message = err_detail.get("message", message)
        except Exception:
            pass

        # Extract Retry-After from headers
        retry_after_header = exc.headers.get("Retry-After") if exc.headers else None
        if retry_after_header:
            try:
                retry_after = float(retry_after_header)
            except (ValueError, TypeError):
                pass

    # Classify by status code
    if status_code == 429:
        return GeminiError(ErrorClass.RETRYABLE, status_code, message, retry_after)

    if status_code in (400, 401, 403):
        return GeminiError(ErrorClass.PERMANENT, status_code, message)

    if status_code == 404:
        return GeminiError(ErrorClass.PERMANENT, status_code, message)

    if status_code and 500 <= status_code < 600:
        return GeminiError(ErrorClass.RETRYABLE, status_code, message)

    # Timeout / connection errors
    msg_lower = message.lower()
    if any(x in msg_lower for x in ("timeout", "timed out", "deadline", "connection refused", "connection reset")):
        return GeminiError(ErrorClass.RETRYABLE, status_code, message)

    if any(x in msg_lower for x in ("429", "rate limit", "quota", "resource exhausted")):
        return GeminiError(ErrorClass.RETRYABLE, status_code, message)

    if any(x in msg_lower for x in ("404", "not found")):
        return GeminiError(ErrorClass.PERMANENT, status_code, message)

    if any(x in msg_lower for x in ("401", "403", "unauthorized", "forbidden", "api key")):
        return GeminiError(ErrorClass.PERMANENT, status_code, message)

    return GeminiError(ErrorClass.UNKNOWN, status_code, message)


class GeminiClient:
    """Stateless Gemini API client. Each call is independent.

    Does NOT manage keys, rotation, or rate limiting.
    The caller (scheduler) provides the specific key and model.
    """

    def __init__(self, model=DEFAULT_MODEL, timeout=REQUEST_TIMEOUT):
        self.model = model
        self.timeout = timeout

    def call(self, prompt, api_key, model=None):
        """Call Gemini API and return parsed JSON output.

        Args:
            prompt: full prompt string (system + user)
            api_key: specific API key to use
            model: override model name (optional)

        Returns:
            dict: parsed JSON response from Gemini

        Raises:
            GeminiError: on any failure (classified as retryable or permanent)
        """
        use_model = model or self.model
        url = API_URL.format(model=use_model) + f"?key={api_key}"

        body = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.15,
                "responseMimeType": "application/json",
            },
        }).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(
                req, timeout=self.timeout, context=SSL_CTX
            ) as resp:
                data = json.loads(resp.read())
        except Exception as exc:
            raise classify_error(exc) from exc

        text = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text")
        )
        if not text:
            raise GeminiError(
                ErrorClass.RETRYABLE, message="Gemini returned no text"
            )

        from analysis.gemini_processor import clean_json
        try:
            return clean_json(text)
        except Exception as exc:
            raise GeminiError(
                ErrorClass.RETRYABLE, message=f"Failed to parse response: {exc}"
            )
