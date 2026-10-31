"""Moved to ``platform_events.events.schema``. See ``outbox/canonical.py``."""

from platform_events.events import schema as _impl

globals().update({k: v for k, v in vars(_impl).items() if not k.startswith("__")})

del _impl
