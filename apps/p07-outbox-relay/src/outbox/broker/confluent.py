"""Adapter for the real broker, via ``confluent-kafka``.

Import-gated like the PostgreSQL store: the dependency is optional and a clear
configuration error beats an ImportError at startup. This is the adapter the
``HA-LIKE`` three-broker lab in design section 16.1 runs against, and it is the
only place durability claims can be made -- :mod:`outbox.broker.embedded` can
reproduce the interleavings but not the replication.

The producer configuration here is not a default to tune later; it is the
durability contract from design section 0.8. ``acks=all`` with
``enable.idempotence=true`` is what makes an acknowledged record survive a
leader change, and ``max.in.flight.requests.per.connection<=5`` is what the
idempotent producer requires to preserve per-partition order on retry. Raising
in-flight above that, or dropping ``acks`` to ``1``, silently converts "at least
once with ordered retries" into something weaker, so both are asserted rather
than merely set.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..errors import ConfigurationError, ErrorCode, MessageTooLarge, ProducerQueueFull
from .ports import DeliveryCallback, DeliveryResult, DeliveryState, Record

#: Required producer settings. A deployment may add to these; overriding one of
#: them changes what "published" means and is refused.
REQUIRED_PRODUCER_CONFIG: dict[str, Any] = {
    "acks": "all",
    "enable.idempotence": True,
}

DEFAULT_PRODUCER_CONFIG: dict[str, Any] = {
    **REQUIRED_PRODUCER_CONFIG,
    "max.in.flight.requests.per.connection": 5,
    "retries": 10,
    "retry.backoff.ms": 200,
    "delivery.timeout.ms": 120_000,
    "linger.ms": 5,
    "compression.type": "lz4",
    # Our own bound is enforced before produce(); this is the library's.
    "queue.buffering.max.kbytes": 8192,
}

DEFAULT_CONSUMER_CONFIG: dict[str, Any] = {
    # Manual commits are mandatory: the offset must be committed *after* the
    # consumer's database transaction, and auto-commit would do it on a timer.
    "enable.auto.commit": False,
    # Never silently jump over history that retention removed.
    "auto.offset.reset": "error",
    "isolation.level": "read_committed",
}


def _require_confluent() -> Any:
    try:
        import confluent_kafka  # noqa: PLC0415 - optional dependency
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised by env
        raise ConfigurationError(
            "the Kafka adapter requires the 'kafka' extra: pip install "
            "'postgres-outbox-kafka[kafka]'"
        ) from exc
    return confluent_kafka


def _validate_config(config: dict[str, Any]) -> dict[str, Any]:
    merged = {**DEFAULT_PRODUCER_CONFIG, **config}
    for key, required in REQUIRED_PRODUCER_CONFIG.items():
        if merged.get(key) != required:
            raise ConfigurationError(
                f"producer config {key}={merged.get(key)!r} must be {required!r}: "
                "the at-least-once guarantee is stated relative to this setting"
            )
    in_flight = int(merged.get("max.in.flight.requests.per.connection", 5))
    if in_flight > 5:
        raise ConfigurationError(
            f"max.in.flight.requests.per.connection={in_flight} exceeds 5, which the "
            "idempotent producer requires in order to preserve per-partition order "
            "across retries"
        )
    if "bootstrap.servers" not in merged:
        raise ConfigurationError("producer config requires bootstrap.servers")
    return merged


def _classify(error: Any) -> tuple[DeliveryState, ErrorCode, str]:
    """Map a librdkafka error onto our taxonomy.

    The ambiguous cases are mapped to ``AMBIGUOUS`` rather than ``FAILED``
    because the distinction drives operations: a timed-out or aborted request
    may well be in the log, so the runbook is "retry the same event_id and
    expect a possible duplicate", not "the event was not delivered".
    """
    confluent_kafka = _require_confluent()
    codes = confluent_kafka.KafkaError
    code = error.code()
    mapping: dict[int, tuple[DeliveryState, ErrorCode]] = {
        codes._TRANSPORT: (DeliveryState.FAILED, ErrorCode.BROKER_UNREACHABLE),
        codes._ALL_BROKERS_DOWN: (DeliveryState.FAILED, ErrorCode.BROKER_UNREACHABLE),
        codes.LEADER_NOT_AVAILABLE: (DeliveryState.FAILED, ErrorCode.BROKER_LEADER_UNAVAILABLE),
        codes.NOT_LEADER_FOR_PARTITION: (DeliveryState.FAILED, ErrorCode.BROKER_LEADER_UNAVAILABLE),
        codes.NOT_ENOUGH_REPLICAS: (DeliveryState.FAILED, ErrorCode.BROKER_NOT_ENOUGH_REPLICAS),
        codes.NOT_ENOUGH_REPLICAS_AFTER_APPEND: (
            # Appended, but not replicated enough to be acknowledged: the record
            # may or may not survive, which is the definition of ambiguous.
            DeliveryState.AMBIGUOUS,
            ErrorCode.BROKER_ACK_AMBIGUOUS,
        ),
        codes.REQUEST_TIMED_OUT: (DeliveryState.AMBIGUOUS, ErrorCode.BROKER_ACK_AMBIGUOUS),
        codes._MSG_TIMED_OUT: (DeliveryState.AMBIGUOUS, ErrorCode.BROKER_ACK_AMBIGUOUS),
        codes._TIMED_OUT: (DeliveryState.AMBIGUOUS, ErrorCode.BROKER_ACK_AMBIGUOUS),
        codes.TOPIC_AUTHORIZATION_FAILED: (DeliveryState.FAILED, ErrorCode.AUTH_DENIED),
        codes.CLUSTER_AUTHORIZATION_FAILED: (DeliveryState.FAILED, ErrorCode.AUTH_DENIED),
        codes.SASL_AUTHENTICATION_FAILED: (DeliveryState.FAILED, ErrorCode.AUTH_DENIED),
        codes.MSG_SIZE_TOO_LARGE: (DeliveryState.FAILED, ErrorCode.MESSAGE_TOO_LARGE),
        codes._MSG_SIZE_TOO_LARGE: (DeliveryState.FAILED, ErrorCode.MESSAGE_TOO_LARGE),
        codes._QUEUE_FULL: (DeliveryState.FAILED, ErrorCode.PRODUCER_QUEUE_FULL),
        codes.UNKNOWN_TOPIC_OR_PART: (DeliveryState.FAILED, ErrorCode.BROKER_UNREACHABLE),
    }
    state, mapped = mapping.get(code, (DeliveryState.AMBIGUOUS, ErrorCode.BROKER_ACK_AMBIGUOUS))
    return state, mapped, str(error)


class ConfluentProducer:
    """``Producer`` port over ``confluent_kafka.Producer``."""

    def __init__(self, config: dict[str, Any]) -> None:
        confluent_kafka = _require_confluent()
        self._producer = confluent_kafka.Producer(_validate_config(config))
        self._queued_bytes = 0
        self._pending = 0

    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        headers: dict[str, str] | None = None,
        on_delivery: DeliveryCallback,
    ) -> None:
        size = len(value)

        def _callback(error: Any, message: Any) -> None:
            self._queued_bytes -= size
            self._pending -= 1
            if error is not None:
                state, code, detail = _classify(error)
                on_delivery(
                    DeliveryResult(state=state, topic=topic, error_code=code, detail=detail)
                )
                return
            on_delivery(
                DeliveryResult(
                    state=DeliveryState.ACKED,
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
            )

        try:
            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=list((headers or {}).items()),
                on_delivery=_callback,
            )
        except BufferError as exc:
            raise ProducerQueueFull(f"librdkafka queue is full: {exc}") from exc
        except Exception as exc:  # pragma: no cover - driver-specific
            confluent_kafka = _require_confluent()
            if isinstance(exc, confluent_kafka.KafkaException):
                _state, code, detail = _classify(exc.args[0])
                if code is ErrorCode.MESSAGE_TOO_LARGE:
                    raise MessageTooLarge(detail) from exc
            raise
        self._queued_bytes += size
        self._pending += 1

    def poll(self, timeout: float = 0.0) -> int:
        return int(self._producer.poll(timeout))

    def flush(self, timeout: float = 10.0) -> int:
        return int(self._producer.flush(timeout))

    def buffer_used_bytes(self) -> int:
        return self._queued_bytes

    def in_flight(self) -> int:
        return self._pending

    def close(self) -> None:
        self._producer.flush(10.0)


class ConfluentConsumer:
    """``Consumer`` port over ``confluent_kafka.Consumer``, manual commits only."""

    def __init__(self, config: dict[str, Any]) -> None:
        confluent_kafka = _require_confluent()
        merged = {**DEFAULT_CONSUMER_CONFIG, **config}
        if merged.get("enable.auto.commit"):
            raise ConfigurationError(
                "enable.auto.commit must stay false: the offset is committed after the "
                "consumer's database transaction, and a timer cannot know when that is"
            )
        if "group.id" not in merged or "bootstrap.servers" not in merged:
            raise ConfigurationError("consumer config requires group.id and bootstrap.servers")
        self._confluent = confluent_kafka
        self._consumer = confluent_kafka.Consumer(merged)
        self._group = merged["group.id"]

    @property
    def group(self) -> str:
        return self._group

    def subscribe(self, topics: Sequence[str]) -> None:
        self._consumer.subscribe(list(topics))

    def poll(self, max_records: int = 1, timeout: float = 0.0) -> list[Record]:
        messages = self._consumer.consume(num_messages=max_records, timeout=timeout)
        records: list[Record] = []
        for message in messages or []:
            if message.error() is not None:
                state, code, detail = _classify(message.error())
                from ..errors import BrokerError

                raise BrokerError(f"{state}: {detail}", code=code)
            records.append(
                Record(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    key=message.key() or b"",
                    value=message.value() or b"",
                    headers={key: _as_text(value) for key, value in (message.headers() or [])},
                )
            )
        return records

    def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        topic_partitions = [
            self._confluent.TopicPartition(topic, partition, offset)
            for (topic, partition), offset in offsets.items()
        ]
        self._consumer.commit(offsets=topic_partitions, asynchronous=False)

    def committed(self, topic: str, partition: int) -> int:
        result = self._consumer.committed([self._confluent.TopicPartition(topic, partition)], timeout=10)
        offset = result[0].offset
        return 0 if offset < 0 else int(offset)

    def assignment(self) -> list[tuple[str, int]]:
        return [(tp.topic, tp.partition) for tp in self._consumer.assignment()]

    def close(self) -> None:
        self._consumer.close()


def _as_text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)
