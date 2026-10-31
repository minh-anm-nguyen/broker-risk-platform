"""Moved to ``platform_events.events.envelope``. See ``outbox/canonical.py``."""

from platform_events.events import envelope as _impl

globals().update({k: v for k, v in vars(_impl).items() if not k.startswith("__")})

del _impl
