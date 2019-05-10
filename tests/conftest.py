import pytest

def pytest_addoption(parser):
    parser.addoption("--s3", action="store_true",
        help="run multi-file filter tests with \"testcruise\" data from S3")
    parser.addoption("--popcycle", action="store_true",
        help="test filter and classify results against results of installed popcycle")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--s3"):
        # --s3 given in cli: do not skip s3 tests
        pass
    else:
        skip_s3 = pytest.mark.skip(reason="need --s3 option to run")
        for item in items:
            if "s3" in item.keywords:
                item.add_marker(skip_s3)

    if config.getoption("--popcycle"):
        # --popcycle given in cli: do not skip popcycle tests
        pass
    else:
        skip_popcycle = pytest.mark.skip(reason="need --popcycle option to run")
        for item in items:
            if "popcycle" in item.keywords:
                item.add_marker(skip_popcycle)
