from setuptools import setup

setup(name='seaflowpy',
    version='0.1.0',
    description='A Python library for processing SeaFlow data.',
    url='http://github.com/armbrustlab/seaflowpy',
    author='Chris T. Berthiaume',
    author_email='chrisbee@uw.edu',
    license='GPL3',
    packages=['seaflowpy'],
    install_requires=[
        'boto3',
        'numpy',
        'pandas'
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'filterevt=seaflowpy.filterevt_cli:main'
        ]
    },
    zip_safe=False
)
