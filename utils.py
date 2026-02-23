"""
Utility functions for Apollo enrichment pipeline.
Includes PII masking, data validation, normalization, and logging setup.
"""

import re
import logging
from typing import Optional, Iterator, List, Any
from datetime import datetime
from urllib.parse import urlparse


def mask_pii(text: str) -> str:
    """
    Mask personally identifiable information (PII) in text for safe logging.

    Masks:
    - Email addresses: user@example.com -> u***@e***.com
    - Phone numbers: +1-234-567-8900 -> +*-***-***-**00

    Args:
        text: Input text containing potential PII

    Returns:
        Text with PII masked
    """
    if not text:
        return text

    # Mask email addresses
    email_pattern = r'\b([a-zA-Z0-9])[a-zA-Z0-9._%+-]*@([a-zA-Z0-9])[a-zA-Z0-9.-]*\.[a-zA-Z]{2,}\b'
    text = re.sub(email_pattern, r'\1***@\2***.com', text)

    # Mask phone numbers (various formats)
    phone_patterns = [
        r'\+\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}',  # International
        r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # US format
        r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # Simple format
    ]

    for pattern in phone_patterns:
        matches = re.finditer(pattern, text)
        for match in matches:
            phone = match.group()
            # Keep first and last 2 digits if long enough
            if len(re.sub(r'[^\d]', '', phone)) >= 4:
                masked = re.sub(r'\d', '*', phone[:-2]) + phone[-2:]
            else:
                masked = '*' * len(phone)
            text = text.replace(phone, masked)

    return text


def extract_domain(url: str) -> Optional[str]:
    """
    Extract clean domain from URL.

    Examples:
        https://www.example.com/path -> example.com
        http://subdomain.example.co.uk -> subdomain.example.co.uk
        example.com -> example.com

    Args:
        url: URL or domain string

    Returns:
        Extracted domain or None if invalid
    """
    if not url:
        return None

    # Add scheme if missing
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path

        # Remove www. prefix if present
        if domain.startswith('www.'):
            domain = domain[4:]

        return domain.lower() if domain else None
    except Exception:
        return None


def get_utc_timestamp() -> str:
    """
    Get current UTC timestamp in ISO 8601 format.

    Returns:
        UTC timestamp string (e.g., "2026-01-29T10:30:45Z")
    """
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')


# RFC 5321 max length for email (254)
EMAIL_MAX_LENGTH = 254


def validate_email(email: Optional[str]) -> bool:
    """
    Validate email address format.

    Args:
        email: Email address to validate (str or None)

    Returns:
        True if valid email format, False otherwise
    """
    if email is None:
        return False
    if not isinstance(email, str) or not email.strip():
        return False
    s = email.strip()
    if len(s) > EMAIL_MAX_LENGTH:
        return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, s))


def normalize_company_name(name: str) -> str:
    """
    Normalize company name for consistency.

    - Strips whitespace
    - Converts to title case
    - Removes extra spaces

    Args:
        name: Company name

    Returns:
        Normalized company name
    """
    if not name:
        return ""

    # Strip and remove extra whitespace
    name = ' '.join(name.strip().split())

    # Title case
    return name.title()


def clean_phone_number(phone: str) -> str:
    """
    Standardize phone number format.

    Args:
        phone: Phone number in any format

    Returns:
        Cleaned phone number
    """
    if not phone:
        return ""

    # Remove all non-digit and non-plus characters
    cleaned = re.sub(r'[^\d+]', '', phone)

    return cleaned


def chunk_list(items: List[Any], chunk_size: int) -> Iterator[List[Any]]:
    """
    Split list into chunks for batch processing.

    Args:
        items: List to chunk
        chunk_size: Size of each chunk

    Yields:
        List chunks of specified size
    """
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def safe_dict_get(d: dict, key_path: str, default: Any = None) -> Any:
    """
    Safely navigate nested dictionary with dot notation.

    Examples:
        safe_dict_get({'person': {'name': 'John'}}, 'person.name') -> 'John'
        safe_dict_get({'person': {}}, 'person.name', 'Unknown') -> 'Unknown'

    Args:
        d: Dictionary to navigate
        key_path: Dot-separated path (e.g., 'person.name.first')
        default: Default value if path not found

    Returns:
        Value at key path or default
    """
    keys = key_path.split('.')
    value = d

    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default

    return value


class PIIMaskingFilter(logging.Filter):
    """
    Logging filter that automatically masks PII in all log records.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Mask PII in log message before output.

        Args:
            record: Log record to filter

        Returns:
            True (always allow record through)
        """
        if isinstance(record.msg, str):
            record.msg = mask_pii(record.msg)

        # Also mask any string arguments
        if record.args:
            record.args = tuple(
                mask_pii(str(arg)) if isinstance(arg, str) else arg
                for arg in record.args
            )

        return True


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_to_console: bool = True
) -> logging.Logger:
    """
    Configure application logging with PII masking.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        log_to_console: Whether to log to console

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("apollo_pipeline")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Add PII masking filter
    pii_filter = PIIMaskingFilter()

    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.addFilter(pii_filter)
        logger.addHandler(console_handler)

    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(pii_filter)
        logger.addHandler(file_handler)

    return logger


def flatten_list_to_string(items: List[Any], separator: str = ", ") -> str:
    """
    Flatten a list into a comma-separated string.

    Args:
        items: List of items to flatten
        separator: Separator string

    Returns:
        Flattened string
    """
    if not items:
        return ""

    return separator.join(str(item) for item in items if item)


def safe_int(value: Any, default: int = 0) -> int:
    """
    Safely convert value to integer.

    Args:
        value: Value to convert
        default: Default value if conversion fails

    Returns:
        Integer value or default
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_str(value: Any, default: str = "") -> str:
    """
    Safely convert value to string.

    Args:
        value: Value to convert
        default: Default string if value is None

    Returns:
        String value or default
    """
    if value is None:
        return default
    return str(value).strip()
