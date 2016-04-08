from .context import seaflowpy as sfp

def test_get_paths_new_style():
    f = sfp.seaflowfile.SeaflowFile("2014-07-04T00-00-02+00-00")
    assert f.file_id == "2014-07-04T00-00-02+00-00"

    f = sfp.seaflowfile.SeaflowFile("2014_185/2014-07-04T00-00-02+00-00")
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"

    f = sfp.seaflowfile.SeaflowFile("foo/2014-07-04T00-00-02+00-00")
    assert f.file_id == "2014-07-04T00-00-02+00-00"

    f = sfp.seaflowfile.SeaflowFile("foo/2014_185/2014-07-04T00-00-02+00-00")
    assert f.file_id == "2014_185/2014-07-04T00-00-02+00-00"

    f = sfp.seaflowfile.SeaflowFile("foo/bar/2014-07-04T00-00-02+00-00")
    assert f.file_id == "2014-07-04T00-00-02+00-00"

    f = sfp.seaflowfile.SeaflowFile("foo/bar/2014-07-04T00-00-02+00-00.gz")
    assert f.file_id == "2014-07-04T00-00-02+00-00"

    f = sfp.seaflowfile.SeaflowFile("foo/bar/2014-07-04T00-00-02+00-00.vct.gz")
    assert f.file_id == "2014-07-04T00-00-02+00-00"

    f = sfp.seaflowfile.SeaflowFile("foo/bar/2014-07-04T00-00-02+00-00.opp.gz")
    assert f.file_id == "2014-07-04T00-00-02+00-00"

    f = sfp.seaflowfile.SeaflowFile("foo/bar/2014-07-04T00-00-02+00-00.evt.vct.gz")
    assert f.file_id == "2014-07-04T00-00-02+00-00.evt"

def test_get_paths_old_style():
    f = sfp.seaflowfile.SeaflowFile("42.evt")
    assert f.file_id == "42.evt"

    f = sfp.seaflowfile.SeaflowFile("2014_185/42.evt")
    assert f.file_id == "2014_185/42.evt"

    f = sfp.seaflowfile.SeaflowFile("foo/42.evt",)
    assert f.file_id == "42.evt"

    f = sfp.seaflowfile.SeaflowFile("foo/2014_185/42.evt")
    assert f.file_id == "2014_185/42.evt"

    evt = sfp.seaflowfile.SeaflowFile("foo/bar/42.evt")
    assert evt.file_id == "42.evt"

    f = sfp.seaflowfile.SeaflowFile("foo/bar/42.evt.gz")
    assert f.file_id == "42.evt"

    f = sfp.seaflowfile.SeaflowFile("foo/bar/42.evt.vct.gz")
    assert f.file_id == "42.evt"

    f = sfp.seaflowfile.SeaflowFile("foo/bar/42.evt.opp.gz")
    assert f.file_id == "42.evt"
