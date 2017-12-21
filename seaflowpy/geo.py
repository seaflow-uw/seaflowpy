"""Geo operations"""
from __future__ import division
from builtins import str

def ddm2dd(ddm):
    """Convert degrees decimal minutes coordinate to decimal degrees.

    Args:
        ddm = two item list: [degrees string, decimal minutes string]
              e.g. ["21", "16.6922"]
    Returns:
        A decimal degree string with precision to 4 decimal places
        e.g. "21.2782". This has resolution down to 11.132 m.
    """
    degrees = int(ddm[0])
    minutes = float(ddm[1]) / 60
    if minutes > 1.0:
        raise ValueError('minutes > 60')
    if degrees > 0:
        dd = degrees + minutes
    else:
        dd = degrees - minutes
    return "%.04f" % dd


def gga2ddm(gga):
    """Convert from GGA coordinate to degree decimal minutes.

    Args:
        gga_string = GGA format coordinate string, e.g. "2116.6922"

    Returns:
        A two-item tuple of of strings (degrees, decimal minutes).
        e.g. ("21", "16.6922")
    """
    try:
        gga = str(gga)
        dot_index = gga.index(".")
        degrees = gga[:dot_index-2]
        decimal_minutes = gga[dot_index-2:]
    except ValueError:
        # If no decimal, degrees should start two from the end of the string
        degrees = gga[:-2]
        decimal_minutes = gga[-2:]
    return (degrees, decimal_minutes)


def gga2dd(gga):
    """Convert from GGA coordinate string to decimal degrees string.

    Precision to 4 decimal places (11.132 m)
    e.g. "2116.6922" -> "21.2782"
    """
    try:
        decimaldegrees = ddm2dd(gga2ddm(gga))
    except ValueError as e:
        if str(e) == "minutes > 60":
            raise ValueError("invalid gga %s. Minutes > 60" % gga)
        raise
    return decimaldegrees

def westify_dd_lon(lon):
    """Make longitude string negative.

    Input data has no E/W sign for longitude, but for some cruises we know
    longitudes must be W (negative). Return a negative version of the lon
    string. e.g. a cruise in western hemisphere will be west
    """
    if lon[0] != "-":
        lon = "-" + lon
    return lon
