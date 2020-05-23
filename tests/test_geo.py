import pytest
import seaflowpy as sfp

# pylint: disable=redefined-outer-name

class TestGGA2DecimalDegrees:
    def test_gga2dd(self):
        # GGA inputs
        gga_lons = [
            "15816.43", "-15816.43",
            "01536.43", "-01536.43",
            "00158.43", "-00158.43",
            "00058.43", "-00058.43",
            "15816", "-15816",
            "01536", "-01536",
            "00158", "-00158",
            "00058", "-00058"
        ]
        gga_lats = [
            "1536.43", "-1536.43",
            "0158.43", "-0158.43",
            "0058.43", "-0058.43",
            "1536", "-1536",
            "0158", "-0158",
            "0058", "-0058"
        ]
        # Expected answers in decimal degrees
        dd_lons = [
            "158.2738", "-158.2738",
            "15.6072", "-15.6072",
            "1.9738", "-1.9738",
            "0.9738", "-0.9738",
            "158.2667", "-158.2667",
            "15.6000", "-15.6000",
            "1.9667", "-1.9667",
            "0.9667", "-0.9667"
        ]
        dd_lats = [
            "15.6072", "-15.6072",
            "1.9738", "-1.9738",
            "0.9738", "-0.9738",
            "15.6000", "-15.6000",
            "1.9667", "-1.9667",
            "0.9667", "-0.9667"
        ]
        for (i, lon) in enumerate(gga_lons):
            assert sfp.geo.ggalon2dd(lon) == dd_lons[i]
        for (i, lat) in enumerate(gga_lats):
            assert sfp.geo.ggalat2dd(lat) == dd_lats[i]

    def test_gga2dd_bad_gga(self):
        bad_lons = [
            "15870.43", "-15870.43",  # minutes > 60
            "05870.43", "-05870.43",  # minutes > 60
            "00870.43", "-00870.43",  # minutes > 60
            "00070.43", "-00070.43",  # minutes > 60
            "15870", "-15870",  # minutes > 60
            "05870", "-05870",  # minutes > 60
            "00870", "-00870",  # minutes > 60
            "00070", "-00070",  # minutes > 60
            "19020.43", "-19020.43",  # degrees > 180
            "115870.43", "-115870.43",  # too many digits before decimal
            "", "NA",  # not numbers
            "1111.45", "-1111.45",  # too few digits before decimal
            "1111", "-1111",  # too few digits before decimal
        ]
        bad_lats = [
            "5870.43", "-5870.43",  # minutes > 60
            "0870.43", "-0870.43",  # minutes > 60
            "0070.43", "-0070.43",  # minutes > 60
            "5870", "-5870",  # minutes > 60
            "0870", "-0870",  # minutes > 60
            "0070", "-0070",  # minutes > 60
            "9220.43", "-9220.43",  # degrees > 90
            "15870.43", "-15870.43",  # too many digits before decimal
            "", "NA",  # not numbers
            "111.45", "-111.45",  # too few digits before decimal
            "111", "-111",  # too few digits before decimal
        ]

        for lon in bad_lons:
            with pytest.raises(ValueError):
                _dd = sfp.geo.ggalon2dd(lon)
        for lat in bad_lats:
            with pytest.raises(ValueError):
                _dd = sfp.geo.ggalat2dd(lat)


class TestGGA:
    def test_is_gga_lat(self):
        gga_lats = [
            "1536.43", "-1536.43",
            "90.50", "-90.50"
        ]
        answers = [
            True, True,
            False, False
        ]

        for (i, lat) in enumerate(gga_lats):
            assert sfp.geo.is_gga_lat(lat) == answers[i]

    def test_is_gga_lon(self):
        gga_lons = [
            "15816.43", "-15816.43",
            "23.57", "-128.45"
        ]
        answers = [
            True, True,
            False, False
        ]
        for (i, lon) in enumerate(gga_lons):
            assert sfp.geo.is_gga_lon(lon) == answers[i]
