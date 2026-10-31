"""Plane separation, bearer authentication and rate limiting.

The data plane may only read. Resetting a run or injecting a fault requires an
admin token, and a data-plane token presenting itself at an admin route gets
403 rather than a confusing 404 (WF-101).
"""
