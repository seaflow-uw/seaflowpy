import pytest
from seaflowpy import time

# pylint: disable=redefined-outer-name

def test_parse_bad_date():
    with pytest.raises(ValueError):
        _ = time.parse_date("2019-11-06T10:32:a05+00:00")


def test_parse_good_date():
    dt = time.parse_date("2019-11-06T10:32:05+00:00")
    assert dt.isoformat() == "2019-11-06T10:32:05+00:00"


def test_parse_good_date_assume_utc():
    dt = time.parse_date("2019-11-06T10:32:05+07:00")
    # Should be parsed as UTC even though it's advertised otherwise
    assert dt.isoformat() == "2019-11-06T10:32:05+00:00"


def test_parse_good_date_notutc():
    dt = time.parse_date("2019-11-06T10:32:05+07:00", assume_utc=False)
    # Should not be parsed as UTC
    assert dt.isoformat() == "2019-11-06T10:32:05+07:00"


def test_parse_good_date_notz():
    dt = time.parse_date("2019-11-06T10:32:05", assume_utc=False)
    # Should be parsed as UTC
    assert dt.isoformat() == "2019-11-06T10:32:05+00:00"
    dt = time.parse_date("2019-11-06T10:32:05", assume_utc=True)
    # Should be parsed as UTC
    assert dt.isoformat() == "2019-11-06T10:32:05+00:00"
