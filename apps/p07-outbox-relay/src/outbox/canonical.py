"""Moved to ``platform_events.canonical``.

Kept as a re-export so Project 07's existing imports keep working unchanged.
The definitions now live in ``packages/platform-events`` because Project 03
has to build the same events inside its own transaction, and a producer
importing the relay would be the wrong direction of dependency.

Names are copied rather than star-imported so that module-private helpers stay
reachable for the code and tests that already use them.
"""

from platform_events import canonical as _impl

globals().update({k: v for k, v in vars(_impl).items() if not k.startswith("__")})

del _impl
