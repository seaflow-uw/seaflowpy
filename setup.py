from setuptools import setup

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError):
    long_description = open('README.md').read()


setup(name='seaflowpy',
    use_scm_version=True,
    description='A Python library for SeaFlow data.',
    long_description=long_description,
    url='http://github.com/armbrustlab/seaflowpy',
    author='Chris T. Berthiaume',
    author_email='chrisbee@uw.edu',
    license='GPL3',
    packages=['seaflowpy'],
    install_requires=[
        'arrow',
        'boto3',
        'numpy',
        'pandas',
        'fabric3',
        'future'
    ],
    setup_requires=['pytest-runner', 'setuptools_scm'],
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'seaflowpy_evtpath2juliandir=seaflowpy.evtpath2juliandir_cli:main',
            'seaflowpy_filter=seaflowpy.filterevt_cli:main',
            'seaflowpy_filter_remote=seaflowpy.filterevt_remote_cli:main',
            'seaflowpy_sds2sfl=seaflowpy.sds2sfl_cli:main',
            'seaflowpy_sfl=seaflowpy.sfl_cli:main'
        ]
    },
    zip_safe=False,
    include_package_data=True
)
