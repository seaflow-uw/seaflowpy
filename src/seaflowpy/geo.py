"""Geo operations"""
import re


ggalat_re = re.compile(r'^(?P<degrees>-?\d{2})(?P<minutes>\d{2}(?:\.\d+)?)$')
ggalon_re = re.compile(r'^(?P<degrees>-?\d{3})(?P<minutes>\d{2}(?:\.\d+)?)$')


def ggalat2dd(coord):
    """GGA latitude string (with +/- for N/S) to decimal degrees string.

    Precision to 4 decimal places (11.132 m)
    e.g. "2116.6922" -> "21.2782"
    """
    match = ggalat_re.match(coord)
    if not match:
        raise ValueError("Invalid GGA latitude string '{}'".format(coord))
    if match:
        degrees = int(match.group('degrees'))
        minutes = float(match.group('minutes'))
        if abs(degrees) > 90 or minutes > 60:
            raise ValueError("Invalid GGA latitude string '{}'".format(coord))
        sign = -1 if match.group('degrees')[0] == '-' else 1
    return "{:.4f}".format(sign * (abs(degrees) + (minutes / 60.0)))


def ggalon2dd(coord):
    """GGA longitude string (with +/- for E/W) to decimal degrees string.

    Precision to 4 decimal places (11.132 m)
    e.g. "2116.6922" -> "21.2782"
    """
    match = ggalon_re.match(coord)
    if not match:
        raise ValueError("Invalid GGA latitude string '{}'".format(coord))
    if match:
        degrees = int(match.group('degrees'))
        minutes = float(match.group('minutes'))
        if abs(degrees) > 180 or minutes > 60:
            raise ValueError("Invalid GGA latitude string '{}'".format(coord))
        sign = -1 if match.group('degrees')[0] == '-' else 1
    return "{:.4f}".format(sign * (abs(degrees) + (minutes / 60.0)))


def is_gga_lat(coord):
    return bool(ggalat_re.match(coord))


def is_gga_lon(coord):
    return bool(ggalon_re.match(coord))
