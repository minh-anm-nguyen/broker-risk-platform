"""Metrics and structured logging.

Two constraints worth stating: metric labels never carry an account, request or
deal id (that is what logs are for, and high-cardinality labels kill a metrics
backend), and no log line ever contains a token or an Authorization header
(WF-107).
"""
