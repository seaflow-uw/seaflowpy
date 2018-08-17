import pytest
import seaflowpy as sfp


def test_invalid_filename():
    with pytest.raises(sfp.errors.EVTFileError):
        f = sfp.seaflowfile.SeaFlowFile("foobar")

def test_invalid_filename_date():
    with pytest.raises(sfp.errors.EVTFileError):
        f = sfp.seaflowfile.SeaFlowFile("2014-07-32T00-00-02+00-00")

def test_new_style():
    f = sfp.seaflowfile.SeaFlowFile("2014-07-04T00-00-02+00-00")
    assert f.path == "2014-07-04T00-00-02+00-00"
    assert f.filename == "2014-07-04T00-00-02+00-00"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.julian == "2014_185"
    assert f.path_julian is None
    assert f.is_old_style == False
    assert f.is_new_style == True

    f = sfp.seaflowfile.SeaFlowFile("2014_185/2014-07-04T00-00-02+00-00")
    assert f.path == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.filename == "2014-07-04T00-00-02+00-00"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.julian == "2014_185"
    assert f.path_julian == "2014_185"
    assert f.is_old_style == False
    assert f.is_new_style == True

    f = sfp.seaflowfile.SeaFlowFile("foo/2014-07-04T00-00-02+00-00")
    assert f.path == "foo/2014-07-04T00-00-02+00-00"
    assert f.filename == "2014-07-04T00-00-02+00-00"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.julian == "2014_185"
    assert f.path_julian is None
    assert f.is_old_style == False
    assert f.is_new_style == True

    f = sfp.seaflowfile.SeaFlowFile("foo/2014_185/2014-07-04T00-00-02+00-00")
    assert f.path == "foo/2014_185/2014-07-04T00-00-02+00-00"
    assert f.filename == "2014-07-04T00-00-02+00-00"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.julian == "2014_185"
    assert f.path_julian == "2014_185"
    assert f.is_old_style == False
    assert f.is_new_style == True

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/2014-07-04T00-00-02+00-00")
    assert f.path == "foo/bar/2014-07-04T00-00-02+00-00"
    assert f.filename == "2014-07-04T00-00-02+00-00"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.julian == "2014_185"
    assert f.path_julian is None
    assert f.is_old_style == False
    assert f.is_new_style == True

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/2014-07-04T00-00-02+00-00.gz")
    assert f.path == "foo/bar/2014-07-04T00-00-02+00-00.gz"
    assert f.filename == "2014-07-04T00-00-02+00-00.gz"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.julian == "2014_185"
    assert f.path_julian is None
    assert f.is_old_style == False
    assert f.is_new_style == True

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/2014-07-04T00-00-02+00-00.vct.gz")
    assert f.path == "foo/bar/2014-07-04T00-00-02+00-00.vct.gz"
    assert f.filename == "2014-07-04T00-00-02+00-00.vct.gz"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.julian == "2014_185"
    assert f.path_julian is None
    assert f.is_old_style == False
    assert f.is_new_style == True

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/2014-07-04T00-00-02+00-00.opp.gz")
    assert f.path == "foo/bar/2014-07-04T00-00-02+00-00.opp.gz"
    assert f.filename == "2014-07-04T00-00-02+00-00.opp.gz"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014-07-04T00-00-02+00-00"
    assert f.julian == "2014_185"
    assert f.path_julian is None
    assert f.is_old_style == False
    assert f.is_new_style == True

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/2014_185/2014-07-04T00-00-02+00-00.opp.gz")
    assert f.path == "foo/bar/2014_185/2014-07-04T00-00-02+00-00.opp.gz"
    assert f.filename == "2014-07-04T00-00-02+00-00.opp.gz"
    assert f.filename_noext == "2014-07-04T00-00-02+00-00"
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.path_file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.julian == "2014_185"
    assert f.path_julian == "2014_185"
    assert f.is_old_style == False
    assert f.is_new_style == True


def test_old_style():
    f = sfp.seaflowfile.SeaFlowFile("42.evt")
    assert f.path == "42.evt"
    assert f.filename == "42.evt"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.julian is None
    assert f.path_julian is None
    assert f.is_old_style == True
    assert f.is_new_style == False

    f = sfp.seaflowfile.SeaFlowFile("2014_185/42.evt")
    assert f.path == "2014_185/42.evt"
    assert f.filename == "42.evt"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "2014_185/42.evt"
    assert f.path_file_id == f.file_id
    assert f.julian is None
    assert f.path_julian == "2014_185"
    assert f.is_old_style == True
    assert f.is_new_style == False

    f = sfp.seaflowfile.SeaFlowFile("foo/42.evt")
    assert f.path == "foo/42.evt"
    assert f.filename == "42.evt"
    assert f.filename_noext == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.file_id == "42.evt"
    assert f.julian is None
    assert f.path_julian is None
    assert f.is_old_style == True
    assert f.is_new_style == False

    f = sfp.seaflowfile.SeaFlowFile("foo/2014_185/42.evt")
    assert f.path == "foo/2014_185/42.evt"
    assert f.filename == "42.evt"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "2014_185/42.evt"
    assert f.path_file_id == f.file_id
    assert f.julian is None
    assert f.path_julian == "2014_185"
    assert f.is_old_style == True
    assert f.is_new_style == False

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/42.evt")
    assert f.path == "foo/bar/42.evt"
    assert f.filename == "42.evt"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.julian is None
    assert f.path_julian is None
    assert f.is_old_style == True
    assert f.is_new_style == False

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/42.evt.gz")
    assert f.path == "foo/bar/42.evt.gz"
    assert f.filename == "42.evt.gz"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.julian is None
    assert f.path_julian is None
    assert f.is_old_style == True
    assert f.is_new_style == False

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/42.evt.vct.gz")
    assert f.path == "foo/bar/42.evt.vct.gz"
    assert f.filename == "42.evt.vct.gz"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.julian is None
    assert f.path_julian is None
    assert f.is_old_style == True
    assert f.is_new_style == False

    f = sfp.seaflowfile.SeaFlowFile("foo/bar/42.evt.opp.gz")
    assert f.path == "foo/bar/42.evt.opp.gz"
    assert f.filename == "42.evt.opp.gz"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "42.evt"
    assert f.path_file_id == f.file_id
    assert f.julian is None
    assert f.path_julian is None
    assert f.is_old_style == True
    assert f.is_new_style == False

    f = sfp.seaflowfile.SeaFlowFile("foo/2014_185/42.evt.opp.gz")
    assert f.path == "foo/2014_185/42.evt.opp.gz"
    assert f.filename == "42.evt.opp.gz"
    assert f.filename_noext == "42.evt"
    assert f.file_id == "2014_185/42.evt"
    assert f.path_file_id == f.file_id
    assert f.julian is None
    assert f.path_julian == "2014_185"
    assert f.is_old_style == True
    assert f.is_new_style == False


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
        "2014_350/2014-12-08T22-51-34+00-00",  # note julian_path from path is wrong
        "2014_342/2014-12-08T22-53-34+00-00",
        "2014-12-08T22-56-34+00-00",
        "2015_186/1.evt"
    ]

    assert sfp.seaflowfile.sorted_files(unsorted_files) == sorted_files


def test_date_from_file_name():
    files = [
        "2014-07-06T00-00-05+00-00",
        "2014-07-06T00-00-05-00-00",
        "2014-07-06T00-00-05-07-00",
        "2014-07-06T00-00-05+00-00.gz"
    ]
    answers = [
        "2014-07-06T00:00:05+00:00",
        "2014-07-06T00:00:05+00:00",
        "2014-07-06T00:00:05-07:00",
        "2014-07-06T00:00:05+00:00"
    ]
    for i, f in enumerate(files):
        s = sfp.seaflowfile.SeaFlowFile(f)
        assert s.rfc3339 == answers[i]


def test_julian_from_filename():
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
    julian_answer = "2014_187"
    path_julian_answer = "2014_001"

    for i, f in enumerate(files[0:4]):
        s = sfp.seaflowfile.SeaFlowFile(f)
        assert s.julian == julian_answer
        assert s.path_julian == None

    for i, f in enumerate(files[4:]):
        s = sfp.seaflowfile.SeaFlowFile(f)
        assert s.julian == julian_answer
        assert s.path_julian == "2014_001"

def test_parse_path():
    files = [
        "2014_187/2014-07-06T00-00-05-00-00",
        "2014-07-06T00-00-05-00-00",
        "2014_187/2014-07-06T00-00-05-00-00.gz",
        "2014-07-06T00-00-05-00-00.gz"
    ]

    answers = [
        { "file": "2014-07-06T00-00-05-00-00", "julian": "2014_187"},
        { "file": "2014-07-06T00-00-05-00-00", "julian": None},
        { "file": "2014-07-06T00-00-05-00-00.gz", "julian": "2014_187"},
        { "file": "2014-07-06T00-00-05-00-00.gz", "julian": None}
    ]

    for i, f in enumerate(files):
        assert sfp.seaflowfile.parse_path(f) == answers[i]

def test_fix_file_id():
    origid = "2014_001/2014-12-08T22-53-34+00-00"
    correctid = "2014_342/2014-12-08T22-53-34+00-00"
    assert sfp.seaflowfile.SeaFlowFile(origid).file_id == correctid
    assert sfp.seaflowfile.SeaFlowFile(origid).path_file_id == origid
