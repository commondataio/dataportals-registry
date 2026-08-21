"""HTTP-verified endpoint inference shared by quality-fix scripts."""


def infer_endpoints(record):
    """Probe catalog APIs via apidetect URL maps; return [] if none respond."""
    from apidetect import infer_endpoints_verified

    return infer_endpoints_verified(record)
