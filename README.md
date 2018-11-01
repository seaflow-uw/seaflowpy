A Python package for SeaFlow flow cytometer data.

### Requirements
seaflowpy has been tested against Python 3.6 and 3.7.

### Install
It's good practice to install packages in a conda environment or virtualenv virtual environment. Here we'll assume we're installing into a conda environment.

```sh
git clone https://github.com/armbrustlab/seaflowpy
cd seaflowpy
# Create a conda environment with only pip installed
conda create -n seaflowpy pip
conda activate seaflowpy
pip install .
# Confirm the seaflowpy command-line tool is accessible
seaflowpy version
```

Experienced users of conda might have expected pandas and numpy to be installed through conda. In this case we're pulling pandas and numpy from PyPI with pip. This is for performance reasons. The default numpy package in conda makes use of the MKL (Math Kernel Library for Intel processors). While this generally results in a performance improvements with numpy, given the way that seaflowpy handles parallel processing MKL ends up significantly degrading multicore performance. To be clear this is most likely not a problem with numpy + MKL, just with the way that seaflowpy interacts with numpy + MKL in their default configuration. Future work will be exploring ways to configure numpy + MKL to play nice with seaflowpy multicore workloads.

### Testing
It is recommended to run tests using `tox`. Install `tox` with `pip`, then from the `seaflowpy` source directory just run `tox`. This will install `seaflowpy` into a temporary virtual environment and run `pytest` tests against that installation.

It's also possible to run `pytest` from the source directory to test directly against the version of `seaflowpy` installed in the current environment.

### Command-line interface
All `seaflowpy` CLI tools are accessible from the `seaflowpy` executable. Run `seaflowpy --help` to begin exploring the CLI usage documentation.
