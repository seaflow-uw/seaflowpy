from functools import wraps
from pathlib import Path
from signal import getsignal, signal, SIGPIPE, SIG_DFL
import sys


def jobs_parts(things, n):
    """Split a list of things into n sublists."""
    if n < 1:
        raise ValueError("n must be > 1")
    n = int(min(len(things), n))
    per_part = len(things) // n
    rem = len(things) % n
    buckets = []
    i = 0
    while i < len(things):
        if rem > 0:
            extra = 1
        else:
            extra = 0
        start = i
        end = i + per_part + extra
        rem -= 1
        i = end
        buckets.append(things[start:end])
    return buckets


def mkdir_p(path):
    """Create directory tree for path."""
    Path(path).mkdir(exist_ok=True, parents=True)


def quantile_str(q):
    """
    Display quantile float as string.

    If there is not decimal part, don't display ".0". If there is a decimal
    part, display it.
    """
    return "{0}".format(str(q) if q % 1 else int(q))


def suppress_sigpipe(f):
    """Decorator to handle SIGPIPE cleanly.

    Prevent Python from turning SIGPIPE into an exception and printing an
    uncatchable error message. Note, if the wrapped function depends on the
    default behavior of Python when handling SIGPIPE this decorator may have
    unintended effects."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        orig_handler = getsignal(SIGPIPE)
        signal(SIGPIPE, SIG_DFL)
        try:
            f(*args, **kwargs)
        finally:
            signal(SIGPIPE, orig_handler)  # restore original Python SIGPIPE handler
    return wrapper


def quiet_keyboardinterrupt(f):
    """Decorator to exit quietly on keyboard interrupt."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except KeyboardInterrupt:
            sys.exit()
    return wrapper


def zerodiv(x, y):
    """Divide x by y, floating point, and default to 0.0 if divisor is 0"""
    try:
        answer = float(x) / float(y)
    except ZeroDivisionError:
        answer = 0.0
    return answer
