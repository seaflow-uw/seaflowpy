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
