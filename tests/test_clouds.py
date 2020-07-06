import pytest
import seaflowpy as sfp

# pylint: disable=redefined-outer-name


@pytest.mark.s3
def test_S3_file_listing():
    """Test S3 multi-file filtering and ensure output can be read back OK"""
    config = sfp.conf.get_aws_config()
    cloud = sfp.clouds.AWS(config.items("aws"))
    files = sorted(cloud.get_files("testcruise_evt"))
    assert files == [
        "testcruise_evt/2014_185/2014-07-04T00-00-00+00-00.sfl",
        "testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
        "testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
        "testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",
        "testcruise_evt/2014_185/2014-07-04T00-09-02+00-00",
        "testcruise_evt/2014_185/2014-07-04T00-12-02+00-00",
        "testcruise_evt/2014_185/2014-07-04T00-15-02+00-00.gz",
        "testcruise_evt/2014_185/2014-07-04T00-17-02+00-00.gz",
        "testcruise_evt/2014_185/2014-07-04T00-21-02+00-00",
        "testcruise_evt/2014_185/2014-07-04T00-27-02+00-00",
        "testcruise_evt/README.md",
    ]
