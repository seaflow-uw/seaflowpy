from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import re
import sys


VERSIONFILE = "src/seaflowpy/_version.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to pytest")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ''

    def run_tests(self):
        import shlex
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(shlex.split(self.pytest_args))
        sys.exit(errno)


setup(
    name='seaflowpy',
    description='A Python library for SeaFlow data.',
    long_description=open('README.md', 'r').read(),
    version=verstr,
    url='https://github.com/armbrustlab/seaflowpy',
    author='Chris T. Berthiaume',
    author_email='chrisbee@uw.edu',
    license='GPL3',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=[
        'boto3',
        'click',
        'numexpr',
        'pandas',
        'fabric3'
    ],
    tests_require=['pytest'],
    cmdclass = {'test': PyTest},
    entry_points={
        'console_scripts': [
            'seaflowpy=seaflowpy.cli.cli:cli'
        ]
    }
)
