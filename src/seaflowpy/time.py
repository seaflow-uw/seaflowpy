"""Module for common SeaFlow datetime operations."""
import datetime
import pytz


def parse_date(date_str, assume_utc=True):
    """
    Parse a SeaFlow timestamp.

    If assume_utc, ignore any timezone and assume UTC. If no timezone offset is
    found in date_str, always set to UTC.

    Returns a datetime.datetime object. Raises ValueError if the timestamp can't
    be parsed.
    """
    # First normalize timezone Z to +00:00
    if date_str.endswith("Z"):
        date_str = date_str[:-1] + "+00:00"
    date = datetime.datetime.fromisoformat(date_str)
    if assume_utc or date.tzinfo is None:
        date = date.replace(tzinfo=pytz.utc)
    return date


def seaflow_rfc3339(date):
    """Standard SeaFlow RFC3339 timestamp."""
    return date.isoformat(timespec='seconds')
