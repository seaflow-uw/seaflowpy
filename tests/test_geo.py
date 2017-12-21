from builtins import object
import numpy.testing as npt
import os
import py.path
import pytest
from .context import seaflowpy as sfp


class TestGGA2DecimalDegrees(object):
    def test_gga2dd(self):
        # GGA inputs
        ggas = [
            "15816.43", "-15816.43",  # pos/neg 3 digit degrees
            "1536.43", "-1536.43", # pos/neg 2 digit degrees
            "158.43", "-158.43", # pos/neg 1 digit degrees
        ]
        # Expected answers in decimal degrees
        dds = [
            "158.2738", "-158.2738",
            "15.6072", "-15.6072",
            "1.9738", "-1.9738"
        ]
        for (i, gga) in enumerate(ggas):
            assert sfp.geo.gga2dd(gga) == dds[i]

    def test_gga2dd_bad_gga(self):
        # Test invalid gga where minutes are > 60
        with pytest.raises(ValueError):
            dd = sfp.geo.gga2dd("158.80.45")
        with pytest.raises(ValueError):
            dd = sfp.geo.gga2dd("1.61")
        with pytest.raises(ValueError):
            dd = sfp.geo.gga2dd("-158.80.45")
        with pytest.raises(ValueError):
            dd = sfp.geo.gga2dd("-1.61")
