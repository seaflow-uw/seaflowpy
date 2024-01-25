from pathlib import Path


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


def zerodiv(x, y):
    """Divide x by y, floating point, and default to 0.0 if divisor is 0"""
    try:
        answer = float(x) / float(y)
    except ZeroDivisionError:
        answer = 0.0
    return answer


def expand_file_list(files_and_dirs: list[str]) -> list[str]:
    """
    Return files_and_dirs with directories replaced by the files they contain
    
    For example, ["file1", "file2", "dir1"] becomes
    ["file1", "file2", "dir1/file3"]. This function does not recurse into
    subdirectories.
    """
    dirs = [f for f in files_and_dirs if Path(f).is_dir()]
    files = [f for f in files_and_dirs if Path(f).is_file()]
    dfiles = []
    for d in dirs:
        dfiles = dfiles + [str(p) for p in Path(d).glob("**/*") if p.is_file()]
    return files + dfiles
