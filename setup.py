import re
from setuptools import setup, find_packages

VERSIONFILE = "src/seaflowpy/_version.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

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
        'pandas',
        'fabric3'
    ],
    entry_points={
        'console_scripts': [
            'seaflowpy=seaflowpy.cli.cli:cli'
        ]
    },
    zip_safe=True
)
