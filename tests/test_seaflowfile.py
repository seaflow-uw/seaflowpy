import pytest
from .context import seaflowpy as sfp


def test_new_style():
    f = sfp.seaflowfile.SeaflowFile("2014-07-04T00-00-02+00-00")
    assert f.file_id == "2014-07-04T00-00-02+00-00"
    assert f.is_old_style() == False

    f = sfp.seaflowfile.SeaflowFile("2014_185/2014-07-04T00-00-02+00-00")
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.is_old_style() == False

    f = sfp.seaflowfile.SeaflowFile("foo/2014-07-04T00-00-02+00-00")
    assert f.file_id == "2014-07-04T00-00-02+00-00"
    assert f.is_old_style() == False

    f = sfp.seaflowfile.SeaflowFile("foo/2014_185/2014-07-04T00-00-02+00-00")
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"
    assert f.is_old_style() == False

    f = sfp.seaflowfile.SeaflowFile("foo/bar/2014-07-04T00-00-02+00-00")
    assert f.file_id == "2014-07-04T00-00-02+00-00"
    assert f.is_old_style() == False

    f = sfp.seaflowfile.SeaflowFile("foo/bar/2014-07-04T00-00-02+00-00.gz")
    assert f.file_id == "2014-07-04T00-00-02+00-00"
    assert f.is_old_style() == False

    f = sfp.seaflowfile.SeaflowFile("foo/bar/2014-07-04T00-00-02+00-00.vct.gz")
    assert f.file_id == "2014-07-04T00-00-02+00-00"
    assert f.is_old_style() == False

    f = sfp.seaflowfile.SeaflowFile("foo/bar/2014-07-04T00-00-02+00-00.opp.gz")
    assert f.file_id == "2014-07-04T00-00-02+00-00"
    assert f.is_old_style() == False

    f = sfp.seaflowfile.SeaflowFile("foo/bar/2014-07-04T00-00-02+00-00.evt.vct.gz")
    assert f.file_id == "2014-07-04T00-00-02+00-00.evt"
    assert f.is_old_style() == False


def test_old_style():
    f = sfp.seaflowfile.SeaflowFile("42.evt")
    assert f.file_id == "42.evt"
    assert f.is_old_style() == True

    f = sfp.seaflowfile.SeaflowFile("2014_185/42.evt")
    assert f.file_id == "2014_185/42.evt"
    assert f.is_old_style() == True

    f = sfp.seaflowfile.SeaflowFile("foo/42.evt",)
    assert f.file_id == "42.evt"
    assert f.is_old_style() == True

    f = sfp.seaflowfile.SeaflowFile("foo/2014_185/42.evt")
    assert f.file_id == "2014_185/42.evt"
    assert f.is_old_style() == True

    evt = sfp.seaflowfile.SeaflowFile("foo/bar/42.evt")
    assert evt.file_id == "42.evt"
    assert f.is_old_style() == True

    f = sfp.seaflowfile.SeaflowFile("foo/bar/42.evt.gz")
    assert f.file_id == "42.evt"
    assert f.is_old_style() == True

    f = sfp.seaflowfile.SeaflowFile("foo/bar/42.evt.vct.gz")
    assert f.file_id == "42.evt"
    assert f.is_old_style() == True

    f = sfp.seaflowfile.SeaflowFile("foo/bar/42.evt.opp.gz")
    assert f.file_id == "42.evt"
    assert f.is_old_style() == True


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


def test_sort_old_chronologically():
    exts = [".opp", ".vct", ".opp.vct"]
    unsorted_files = [
        "2014_186/100.evt",
        "2014_185/10.evt",
        "2014_185/1.evt",
        "2014_186/23.evt"
    ]
    sorted_files = [
        "2014_185/1.evt",
        "2014_185/10.evt",
        "2014_186/23.evt",
        "2014_186/100.evt"
    ]

    assert sfp.seaflowfile.sorted_files(unsorted_files) == sorted_files
    for e in exts:
        files = [f + e for f in unsorted_files]
        answer = [f + e for f in sorted_files]
        assert sfp.seaflowfile.sorted_files(files) == answer
        files = [f + e + ".gz" for f in unsorted_files]
        answer = [f + e + ".gz" for f in sorted_files]
        assert sfp.seaflowfile.sorted_files(files) == answer


def test_find_file_index():
    files = [
        "foo/2014_186/2014-07-06T00-00-05+00-00",
        "foo/2014_185/2014-07-04T10-00-02+00-00",
        "foo/2014_185/2014-07-04T01-00-02+00-00",
        "foo/2014_186/2014-07-05T00-00-02+00-00"
    ]
    i = sfp.seaflowfile.find_file_index(files, "./2014_185/2014-07-04T01-00-02+00-00")
    assert i == 2
    with pytest.raises(ValueError):
        sfp.seaflowfile.find_file_index(files, "./2014_180/2014-07-04T01-00-02+00-00")
    with pytest.raises(ValueError):
        sfp.seaflowfile.find_file_index(files, None)


def test_files_between():
    files = [
        "foo/2014_186/2014-07-06T00-00-05+00-00",
        "foo/2014_185/2014-07-04T10-00-02+00-00",
        "foo/2014_185/2014-07-04T01-00-02+00-00",
        "foo/2014_186/2014-07-05T00-00-02+00-00"
    ]

    # First to Last
    subfiles = sfp.seaflowfile.files_between(
        files,
        "2014_185/2014-07-04T01-00-02+00-00",
        "2014_186/2014-07-06T00-00-05+00-00"
    )
    assert subfiles == sfp.seaflowfile.sorted_files(files)

    # None to None
    subfiles = sfp.seaflowfile.files_between(
        files,
        None,
        None
    )
    assert subfiles == sfp.seaflowfile.sorted_files(files)

    # None to second
    subfiles = sfp.seaflowfile.files_between(
        files,
        None,
        "2014_185/2014-07-04T10-00-02+00-00"
    )
    assert subfiles == sfp.seaflowfile.sorted_files(files)[:2]

    # second to None
    subfiles = sfp.seaflowfile.files_between(
        files,
        "2014_185/2014-07-04T10-00-02+00-00",
        None
    )
    assert subfiles == sfp.seaflowfile.sorted_files(files)[1:]

    # unmatched to unmatched
    subfiles = sfp.seaflowfile.files_between(
        files,
        "foo",
        "bar"
    )
    assert subfiles == sfp.seaflowfile.sorted_files(files)
