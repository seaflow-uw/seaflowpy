import datetime

import pandas as pd
import pytest
import seaflowpy as sfp

# pylint: disable=redefined-outer-name


def test_invalid_filename():
    with pytest.raises(sfp.errors.FileError):
        _ = sfp.seaflowfile.SeaFlowFile("foobar")

def test_invalid_filename_date():
    with pytest.raises(sfp.errors.FileError):
        _ = sfp.seaflowfile.SeaFlowFile("2014-07-32T00-00-02+00-00")

def test_new_style():
    f = sfp.seaflowfile.SeaFlowFile("2014-07-04T00-00-02+00-00")
    assert f.path == "2014-07-04T00-00-02+00-00"
    assert f.filename == "2014-07-04T00-00-02+00-00"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.dayofyear == "2014_185"
    assert f.path_dayofyear == ''
    assert f.is_old_style is False
    assert f.is_new_style is True

    f = sfp.seaflowfile.SeaFlowFile("2014_185/2014-07-04T00-00-02+00-00")
    assert f.path == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.filename == "2014-07-04T00-00-02+00-00"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.dayofyear == "2014_185"
    assert f.path_dayofyear == "2014_185"
    assert f.is_old_style is False
    assert f.is_new_style is True

    f = sfp.seaflowfile.SeaFlowFile("foo/2014-07-04T00-00-02+00-00")
    assert f.path == "foo/2014-07-04T00-00-02+00-00"
    assert f.filename == "2014-07-04T00-00-02+00-00"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.dayofyear == "2014_185"
    assert f.path_dayofyear == ''
    assert f.is_old_style is False
    assert f.is_new_style is True

    f = sfp.seaflowfile.SeaFlowFile("foo/2014_185/2014-07-04T00-00-02+00-00")
    assert f.path == "foo/2014_185/2014-07-04T00-00-02+00-00"
    assert f.filename == "2014-07-04T00-00-02+00-00"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.dayofyear == "2014_185"
    assert f.path_dayofyear == "2014_185"
    assert f.is_old_style is False
    assert f.is_new_style is True

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/2014-07-04T00-00-02+00-00")
    assert f.path == "foo/bar/2014-07-04T00-00-02+00-00"
    assert f.filename == "2014-07-04T00-00-02+00-00"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.dayofyear == "2014_185"
    assert f.path_dayofyear == ''
    assert f.is_old_style is False
    assert f.is_new_style is True

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/2014-07-04T00-00-02+00-00.gz")
    assert f.path == "foo/bar/2014-07-04T00-00-02+00-00.gz"
    assert f.filename == "2014-07-04T00-00-02+00-00.gz"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.dayofyear == "2014_185"
    assert f.path_dayofyear == ''
    assert f.is_old_style is False
    assert f.is_new_style is True

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/2014-07-04T00-00-02+00-00.vct.gz")
    assert f.path == "foo/bar/2014-07-04T00-00-02+00-00.vct.gz"
    assert f.filename == "2014-07-04T00-00-02+00-00.vct.gz"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.dayofyear == "2014_185"
    assert f.path_dayofyear == ''
    assert f.is_old_style is False
    assert f.is_new_style is True

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/2014-07-04T00-00-02+00-00.opp.gz")
    assert f.path == "foo/bar/2014-07-04T00-00-02+00-00.opp.gz"
    assert f.filename == "2014-07-04T00-00-02+00-00.opp.gz"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.dayofyear == "2014_185"
    assert f.path_dayofyear == ''
    assert f.is_old_style is False
    assert f.is_new_style is True

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/2014_185/2014-07-04T00-00-02+00-00.opp.gz")
    assert f.path == "foo/bar/2014_185/2014-07-04T00-00-02+00-00.opp.gz"
    assert f.filename == "2014-07-04T00-00-02+00-00.opp.gz"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.dayofyear == "2014_185"
    assert f.path_dayofyear == "2014_185"
    assert f.is_old_style is False
    assert f.is_new_style is True


def test_old_style():
    f = sfp.seaflowfile.SeaFlowFile("42.evt")
    assert f.path == "42.evt"
    assert f.filename == "42.evt"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.dayofyear == ''
    assert f.path_dayofyear == ''
    assert f.is_old_style is True
    assert f.is_new_style is False

    f = sfp.seaflowfile.SeaFlowFile("2014_185/42.evt")
    assert f.path == "2014_185/42.evt"
    assert f.filename == "42.evt"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "2014_185/42.evt"
    assert f.path_file_id == f.file_id
    assert f.dayofyear == ''
    assert f.path_dayofyear == "2014_185"
    assert f.is_old_style is True
    assert f.is_new_style is False

    f = sfp.seaflowfile.SeaFlowFile("foo/42.evt")
    assert f.path == "foo/42.evt"
    assert f.filename == "42.evt"
    assert f.filename_noext == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.file_id == "42.evt"
    assert f.dayofyear == ''
    assert f.path_dayofyear == ''
    assert f.is_old_style is True
    assert f.is_new_style is False

    f = sfp.seaflowfile.SeaFlowFile("foo/2014_185/42.evt")
    assert f.path == "foo/2014_185/42.evt"
    assert f.filename == "42.evt"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "2014_185/42.evt"
    assert f.path_file_id == f.file_id
    assert f.dayofyear == ''
    assert f.path_dayofyear == "2014_185"
    assert f.is_old_style is True
    assert f.is_new_style is False

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/42.evt")
    assert f.path == "foo/bar/42.evt"
    assert f.filename == "42.evt"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.dayofyear == ''
    assert f.path_dayofyear == ''
    assert f.is_old_style is True
    assert f.is_new_style is False

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/42.evt.gz")
    assert f.path == "foo/bar/42.evt.gz"
    assert f.filename == "42.evt.gz"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.dayofyear == ''
    assert f.path_dayofyear == ''
    assert f.is_old_style is True
    assert f.is_new_style is False

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/42.evt.vct.gz")
    assert f.path == "foo/bar/42.evt.vct.gz"
    assert f.filename == "42.evt.vct.gz"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.dayofyear == ''
    assert f.path_dayofyear == ''
    assert f.is_old_style is True
    assert f.is_new_style is False

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/42.evt.opp.gz")
    assert f.path == "foo/bar/42.evt.opp.gz"
    assert f.filename == "42.evt.opp.gz"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.dayofyear == ''
    assert f.path_dayofyear == ''
    assert f.is_old_style is True
    assert f.is_new_style is False

    f = sfp.seaflowfile.SeaFlowFile("foo/2014_185/42.evt.opp.gz")
    assert f.path == "foo/2014_185/42.evt.opp.gz"
    assert f.filename == "42.evt.opp.gz"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "2014_185/42.evt"
    assert f.path_file_id == f.file_id
    assert f.dayofyear == ''
    assert f.path_dayofyear == "2014_185"
    assert f.is_old_style is True
    assert f.is_new_style is False


def test_set_date():
    # Should do nothing for new style if matches
    date = datetime.datetime.fromisoformat("2014-07-04T00:00:02+00:00")
    f = sfp.seaflowfile.SeaFlowFile("2014_185/2014-07-04T00-00-02+00-00", date=date)
    assert f.rfc3339 == "2014-07-04T00:00:02+00:00"

    # Should raise ValueError for new style if doesn't match
    date = datetime.datetime.fromisoformat("2014-07-05T00:00:02+00:00")
    with pytest.raises(ValueError):
        _ = sfp.seaflowfile.SeaFlowFile("2014_185/2014-07-04T00-00-02+00-00", date=date)

    # Should set data for old style
    date = datetime.datetime.fromisoformat("2014-07-04T02:06:00+00:00")
    f = sfp.seaflowfile.SeaFlowFile("2014_185/42.evt", date=date)
    assert f.file_id == "2014_185/42.evt"
    assert f.dayofyear == "2014_185"
    assert f.rfc3339 == "2014-07-04T02:06:00+00:00"


def test_sort_new_chronologically():
    exts = [".opp", ".vct", ".opp.vct"]
    unsorted_files = [
        "2014_186/2014-07-06T00-00-05+00-00",
        "2014_185/2014-07-04T10-00-02+00-00",
        "2014_185/2014-07-04T01-00-02+00-00",
        "2014_186/2014-07-05T00-00-02+00-00"
    ]
    sorted_files = [
        "2014_185/2014-07-04T01-00-02+00-00",
        "2014_185/2014-07-04T10-00-02+00-00",
        "2014_186/2014-07-05T00-00-02+00-00",
        "2014_186/2014-07-06T00-00-05+00-00"
    ]

    assert sfp.seaflowfile.sorted_files(unsorted_files) == sorted_files
    for e in exts:
        files = [f + e for f in unsorted_files]
        answer = [f + e for f in sorted_files]
        assert sfp.seaflowfile.sorted_files(files) == answer
        files = [f + e + ".gz" for f in unsorted_files]
        answer = [f + e + ".gz" for f in sorted_files]
        assert sfp.seaflowfile.sorted_files(files) == answer


def test_sorted_files():
    unsorted_files = [
        "2014_186/100.evt",
        "2014-12-08T22-56-34+00-00",
        "2014_185/10.evt",
        "9.evt",
        "2014_342/2014-12-08T22-53-34+00-00",
        "2014_350/2014-12-08T22-51-34+00-00",
        "2014_185/1.evt",
        "2014_186/23.evt",
        "2015_186/1.evt"
    ]
    sorted_files = [
        "9.evt",
        "2014_185/1.evt",
        "2014_185/10.evt",
        "2014_186/23.evt",
        "2014_186/100.evt",
        "2014_350/2014-12-08T22-51-34+00-00",  # note dayofyear_path from path is wrong
        "2014_342/2014-12-08T22-53-34+00-00",
        "2014-12-08T22-56-34+00-00",
        "2015_186/1.evt"
    ]

    assert sfp.seaflowfile.sorted_files(unsorted_files) == sorted_files


def test_date_from_file_name():
    files = [
        "2014-07-06T00-00-05+00-00",
        "2014-07-06T00-00-05-00-00",
        "2014-07-06T00-00-05-07-00",  # all dates assumed UTC, even if non-UTC TZ offset
        "2014-07-06T00-00-05+00-00.gz"
    ]
    answers = [
        "2014-07-06T00:00:05+00:00",
        "2014-07-06T00:00:05+00:00",
        "2014-07-06T00:00:05+00:00",  # all dates assumed UTC, even if non-UTC TZ offset
        "2014-07-06T00:00:05+00:00"
    ]
    for i, f in enumerate(files):
        s = sfp.seaflowfile.SeaFlowFile(f)
        assert s.rfc3339 == answers[i]


def test_dayofyear_from_filename():
    files = [
        "2014-07-06T00-00-05+00-00",
        "2014-07-06T00-00-05-00-00",
        "2014-07-06T00-00-05-07-00",
        "2014-07-06T00-00-05+00-00.gz",
        "2014_001/2014-07-06T00-00-05+00-00",
        "2014_001/2014-07-06T00-00-05-00-00",
        "2014_001/2014-07-06T00-00-05-07-00",
        "2014_001/2014-07-06T00-00-05+00-00.gz"
    ]
    dayofyear_answer = "2014_187"
    path_dayofyear_answer = "2014_001"

    for f in files[0:4]:
        s = sfp.seaflowfile.SeaFlowFile(f)
        assert s.dayofyear == dayofyear_answer
        assert s.path_dayofyear == ''

    for f in files[4:]:
        s = sfp.seaflowfile.SeaFlowFile(f)
        assert s.dayofyear == dayofyear_answer
        assert s.path_dayofyear == path_dayofyear_answer

def test_parse_path():
    files = [
        "2014_187/2014-07-06T00-00-05-00-00",
        "2014-07-06T00-00-05-00-00",
        "2014_187/2014-07-06T00-00-05-00-00.gz",
        "2014-07-06T00-00-05-00-00.gz"
    ]

    answers = [
        {"file": "2014-07-06T00-00-05-00-00", "dayofyear": "2014_187"},
        {"file": "2014-07-06T00-00-05-00-00", "dayofyear": ''},
        {"file": "2014-07-06T00-00-05-00-00.gz", "dayofyear": "2014_187"},
        {"file": "2014-07-06T00-00-05-00-00.gz", "dayofyear": ''}
    ]

    for i, f in enumerate(files):
        assert sfp.seaflowfile.parse_path(f) == answers[i]

def test_fix_file_id():
    origid = "2014_001/2014-12-08T22-53-34+00-00"
    correctid = "2014_342/2014-12-08T22-53-34+00-00"
    assert sfp.seaflowfile.SeaFlowFile(origid).file_id == correctid
    assert sfp.seaflowfile.SeaFlowFile(origid).path_file_id == origid


def test_file_list_filter():
    files = [
        "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-15-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-17-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-21-02+00-00"
    ]
    filter_list = [
        "2014_185/2014-07-04T00-00-02+00-00",
        "testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
        "foo/2014_185/2014-07-04T00-06-02+00-00",
        "2014_185/2014-07-04T00-17-02+00-00.gz",
        "2014_185/2014-07-04T00-30-02+00-00.gz"
    ]
    answer = [
        "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-17-02+00-00.gz"
    ]
    result = sfp.seaflowfile.filtered_file_list(files, filter_list)
    assert answer == result

evt_files = [
    "testcruise/2014_185/2014-07-04T00-00-02+00-00",
    "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
    "testcruise/2014_185/100.evt",
    "testcruise/2014_185/200.evt.gz",
    "2014_185/2014-07-04T00-00-02+00-00",
    "2014_185/2014-07-04T00-00-02+00-00.gz",
    "2014-07-04T00-00-02+00-00",
    "2014-07-04T00-00-02+00-00.gz"
]
opp_files = [
    "testcruise/2014_185/2014-07-04T00-00-02+00-00.opp",
    "testcruise/2014_185/2014-07-04T00-03-02+00-00.opp.gz",
    "testcruise/2014_185/100.evt.opp",
    "testcruise/2014_185/200.evt.opp.gz",
    "2014_185/2014-07-04T00-00-02+00-00.opp",
    "2014_185/2014-07-04T00-00-02+00-00.opp.gz",
    "2014-07-04T00-00-02+00-00.opp",
    "2014-07-04T00-00-02+00-00.opp.gz"
]

def test_is_evt():
    results = [sfp.seaflowfile.SeaFlowFile(f).is_evt for f in evt_files]
    assert results == [True] * len(evt_files)
    results = [sfp.seaflowfile.SeaFlowFile(f).is_evt for f in opp_files]
    assert results == [False] * len(opp_files)

def test_is_opp():
    results = [sfp.seaflowfile.SeaFlowFile(f).is_opp for f in opp_files]
    assert results == [True] * len(opp_files)
    results = [sfp.seaflowfile.SeaFlowFile(f).is_opp for f in evt_files]
    assert results == [False] * len(evt_files)

def test_keep_evt_files():
    files = [
        "testcruise/2014_185/100.evt",
        "testcruise/2014_185/200.evt.gz",
        "not_evt_file",
        "testcruise/2014_185/2014-07-04T00-00-02+00-00",
        "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
    ]
    parsed = sfp.seaflowfile.keep_evt_files(files)
    assert parsed == (files[:2] + files[3:])

def test_find_evt_files():
    files = sfp.seaflowfile.find_evt_files("tests/testcruise_evt")
    answer = [
        "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-15-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-17-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-21-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-27-02+00-00"
    ]
    assert files == answer

def test_timeselect_evt_files():
    raw_files = [
        "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00"
    ]
    files = [sfp.seaflowfile.SeaFlowFile(f) for f in raw_files]

    # Exact boundaries, test inclusivity
    time_start = datetime.datetime.fromisoformat("2014-07-04T00:03:02+00:00")
    time_end = datetime.datetime.fromisoformat("2014-07-04T00:06:02+00:00")
    selected = sfp.seaflowfile.timeselect_evt_files(files, time_start, time_end)
    answer = [
        "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00"
    ]
    selected = [f.path for f in selected]
    assert selected == answer

    # Inexact boundaries
    time_start = datetime.datetime.fromisoformat("2014-07-04T00:03:00+00:00")
    time_end = datetime.datetime.fromisoformat("2014-07-04T00:07:00+00:00")
    selected = sfp.seaflowfile.timeselect_evt_files(files, time_start, time_end)
    answer = [
        "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00"
    ]
    selected = [f.path for f in selected]
    assert selected == answer

    # No start
    time_end = datetime.datetime.fromisoformat("2014-07-04T00:06:00+00:00")
    selected = sfp.seaflowfile.timeselect_evt_files(files, None, time_end)
    answer = [
        "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz"
    ]
    selected = [f.path for f in selected]
    assert selected == answer

    # No end
    time_start = datetime.datetime.fromisoformat("2014-07-04T00:06:00+00:00")
    selected = sfp.seaflowfile.timeselect_evt_files(files, time_start, None)
    answer = [
        "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00"
    ]
    selected = [f.path for f in selected]
    assert selected == answer

    # Nothing is selected
    time_start = datetime.datetime.fromisoformat("2014-07-01T00:03:02+00:00")
    time_end = datetime.datetime.fromisoformat("2014-07-03T00:06:02+00:00")
    selected = sfp.seaflowfile.timeselect_evt_files(files, time_start, time_end)
    assert selected == []

    # No dates
    selected = sfp.seaflowfile.timeselect_evt_files(files, None, None)
    selected = [f.path for f in selected]
    assert selected == raw_files

    # No tz offset
    time_start = datetime.datetime.fromisoformat("2014-07-04T00:03:02")
    time_end = datetime.datetime.fromisoformat("2014-07-04T00:06:02")
    with pytest.raises(TypeError):
        selected = sfp.seaflowfile.timeselect_evt_files(files, time_start, time_end)

    # Set date to None for one of the files
    files_tmp = [sfp.seaflowfile.SeaFlowFile(f) for f in raw_files]
    files_tmp[2].date = None  # erase date
    time_start = datetime.datetime.fromisoformat("2014-07-04T00:06:00+00:00")
    selected = sfp.seaflowfile.timeselect_evt_files(files_tmp, time_start, None)
    answer = ["tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00"]
    selected = [f.path for f in selected]
    assert selected == answer


def test_date_evt_files():
    evt_files = [
        "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-15-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-17-02+00-00.gz",
        "tests/testcruise_evt/2014_185/2014-07-04T00-21-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-27-02+00-00"
    ]
    sfl_df = pd.read_parquet("tests/sfl.parquet")
    files_df = sfp.seaflowfile.date_evt_files(evt_files, sfl_df)
    answer_df = pd.DataFrame({
        'date': {
            0: pd.Timestamp('2014-07-04 00:00:02+0000', tz='UTC'),
            1: pd.Timestamp('2014-07-04 00:03:02+0000', tz='UTC'),
            2: pd.Timestamp('2014-07-04 00:06:02+0000', tz='UTC'),
            3: pd.Timestamp('2014-07-04 00:09:02+0000', tz='UTC'),
            4: pd.Timestamp('2014-07-04 00:12:02+0000', tz='UTC'),
            5: pd.Timestamp('2014-07-04 00:15:02+0000', tz='UTC'),
            6: pd.Timestamp('2014-07-04 00:17:02+0000', tz='UTC')
        },
        'file_id': {
            0: '2014_185/2014-07-04T00-00-02+00-00',
            1: '2014_185/2014-07-04T00-03-02+00-00',
            2: '2014_185/2014-07-04T00-06-02+00-00',
            3: '2014_185/2014-07-04T00-09-02+00-00',
            4: '2014_185/2014-07-04T00-12-02+00-00',
            5: '2014_185/2014-07-04T00-15-02+00-00',
            6: '2014_185/2014-07-04T00-17-02+00-00'
        },
        'path': {
            0: 'tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00',
            1: 'tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz',
            2: 'tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00',
            3: 'tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00',
            4: 'tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00',
            5: 'tests/testcruise_evt/2014_185/2014-07-04T00-15-02+00-00.gz',
            6: 'tests/testcruise_evt/2014_185/2014-07-04T00-17-02+00-00.gz'}
        }
    )
    hash_result = pd.util.hash_pandas_object(files_df, index=False).sum()
    hash_answer = pd.util.hash_pandas_object(answer_df, index=False).sum()
    assert hash_result == hash_answer
