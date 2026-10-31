"""Moved to ``platform_events.events``. See ``outbox/canonical.py``."""

from platform_events import events as _impl

globals().update({k: v for k, v in vars(_impl).items() if not k.startswith("__")})

del _impl
