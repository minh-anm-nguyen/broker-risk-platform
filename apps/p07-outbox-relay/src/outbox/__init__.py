"""Project 07 — PostgreSQL Outbox & Kafka.

Transactional outbox write (inside the business transaction), an at-least-once
relay to Kafka with lease fencing, and an idempotent reference consumer whose
business effect is applied exactly once per logical event.

What this package proves and what it does not is documented in README.md. The
short version: the outbox/claim/CAS/consumer-inbox logic runs against a real
ACID database, so atomicity and fencing are genuinely exercised; the default
broker is an in-process Kafka *model*, so ISR/replication durability is
modelled, not proven.
"""

__version__ = "1.0.0"
