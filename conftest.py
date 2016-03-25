import pytest

def pytest_addoption(parser):
    parser.addoption("--s3", action="store_true",
        help="run multi-file filter tests with \"testcruise\" data from S3")
