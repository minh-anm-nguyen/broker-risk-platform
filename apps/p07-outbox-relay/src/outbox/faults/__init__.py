"""Fault injection hooks shared by the writer, the relay and the consumer."""

from .hooks import HOOKS, CrashSignal, HookRegistry, KillPoint, fire

__all__ = ["HOOKS", "CrashSignal", "HookRegistry", "KillPoint", "fire"]
