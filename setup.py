from setuptools import setup, find_packages
import versioneer

setup(
    name='seaflowpy',
    description='A Python library for SeaFlow data.',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    url='https://github.com/armbrustlab/seaflowpy',
    author='Chris T. Berthiaume',
    author_email='chrisbee@uw.edu',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    python_requires='>=3.7.1',
    install_requires=[
        'boto3',
        'click',
        'fabric3',
        'hdbscan',
        'kern-smooth',
        'matplotlib',
        'pandas',
        'pyarrow',
        'tsdataformat'
    ],
    entry_points={
        'console_scripts': [
            'seaflowpy=seaflowpy.cli.cli:cli'
        ]
    },
    classifiers=[
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)'
    ],
    zip_safe=True
)
