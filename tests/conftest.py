import pytest

def pytest_addoption(parser):
    parser.addoption("--popcycle", action="store_true",
        help="test filter and classify results against results of installed popcycle")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--popcycle"):
        # --popcycle given in cli: do not skip popcycle tests
        pass
    else:
        skip_popcycle = pytest.mark.skip(reason="need --popcycle option to run")
        for item in items:
            if "popcycle" in item.keywords:
                item.add_marker(skip_popcycle)
