"""
Tests for StreamSourceAdapter protocol, StreamResult, and concrete source stubs.

Phase 1 coverage targets
------------------------
sqldim/sources/stream.py     → ~100%
sqldim/sources/kafka.py      → ~80%  (constructor + public API; no real Kafka)
sqldim/sources/kinesis.py    → ~80%  (constructor + public API; no real AWS)
sqldim/sources/cdc.py        → ~80%  (constructor + public API; no real CDC)
"""
from __future__ import annotations

import pytest
import duckdb

from sqldim.sources.stream import StreamSourceAdapter, StreamResult
from sqldim.sources.kafka import KafkaSource
from sqldim.sources.kinesis import KinesisSource
from sqldim.sources.cdc import DebeziumSource
from sqldim.scd.handler import SCDResult


# ---------------------------------------------------------------------------
# Minimal stub that satisfies the protocol
# ---------------------------------------------------------------------------

class StubStreamSource:
    """Finite stream over a list of pre-registered DuckDB view names."""

    def __init__(self, views: list[str]):
        self._views = views
        self._idx = 0

    def stream(self, con, batch_size=10_000):
        for view in self._views:
            self._idx += 1
            yield f"SELECT * FROM {view}"

    def commit(self, offset) -> None:
        pass

    def checkpoint(self):
        return self._idx


class IncompleteSource:
    """Missing commit() — must NOT satisfy the protocol."""

    def stream(self, con, batch_size=10_000):
        yield "SELECT 1"

    def checkpoint(self):
        return 0


# ---------------------------------------------------------------------------
# StreamSourceAdapter protocol checks
# ---------------------------------------------------------------------------

class TestStreamSourceAdapterProtocol:
    def test_stub_satisfies_protocol(self):
        assert isinstance(StubStreamSource([]), StreamSourceAdapter)

    def test_incomplete_source_fails_protocol(self):
        assert not isinstance(IncompleteSource(), StreamSourceAdapter)

    def test_plain_object_fails_protocol(self):
        assert not isinstance(object(), StreamSourceAdapter)

    def test_string_fails_protocol(self):
        assert not isinstance("SELECT 1", StreamSourceAdapter)

    def test_kafka_source_satisfies_protocol(self):
        src = KafkaSource(brokers="localhost:9092", topic="t", group_id="g")
        assert isinstance(src, StreamSourceAdapter)

    def test_kinesis_source_satisfies_protocol(self):
        src = KinesisSource(stream_name="my-stream")
        assert isinstance(src, StreamSourceAdapter)

    def test_debezium_source_satisfies_protocol(self):
        kafka = KafkaSource(brokers="localhost:9092", topic="t", group_id="g")
        src = DebeziumSource(kafka_source=kafka, natural_key="id")
        assert isinstance(src, StreamSourceAdapter)


# ---------------------------------------------------------------------------
# StreamResult accumulation
# ---------------------------------------------------------------------------

class TestStreamResult:
    def test_initial_state(self):
        r = StreamResult()
        assert r.inserted == 0
        assert r.versioned == 0
        assert r.unchanged == 0
        assert r.batches_processed == 0
        assert r.batches_failed == 0

    def test_accumulate_adds_all_fields(self):
        r = StreamResult()
        scd = SCDResult()
        scd.inserted = 3
        scd.versioned = 2
        scd.unchanged = 5
        r.accumulate(scd)
        assert r.inserted == 3
        assert r.versioned == 2
        assert r.unchanged == 5

    def test_accumulate_sums_across_batches(self):
        r = StreamResult()
        for _ in range(4):
            scd = SCDResult()
            scd.inserted = 1
            scd.versioned = 1
            scd.unchanged = 1
            r.accumulate(scd)
        assert r.inserted == 4
        assert r.versioned == 4
        assert r.unchanged == 4

    def test_batches_processed_not_touched_by_accumulate(self):
        r = StreamResult()
        r.accumulate(SCDResult())
        # accumulate() does not increment batches_processed; that is the
        # caller's (process_stream) responsibility.
        assert r.batches_processed == 0

    def test_stream_result_is_dataclass(self):
        # Must be constructable without arguments
        StreamResult()

    def test_stream_result_fields_are_additive(self):
        a = StreamResult()
        a.inserted = 10
        a.versioned = 5
        a.unchanged = 20
        a.batches_processed = 2
        a.batches_failed = 1

        b = StreamResult()
        b.inserted = 7
        b.versioned = 3
        b.unchanged = 8
        b.batches_processed = 1
        b.batches_failed = 0

        # Not a built-in merge — just verify field access
        total_inserted = a.inserted + b.inserted
        assert total_inserted == 17


# ---------------------------------------------------------------------------
# KafkaSource — constructor and public API (no real Kafka)
# ---------------------------------------------------------------------------

class TestKafkaSource:
    def test_basic_construction(self):
        src = KafkaSource(
            brokers="localhost:9092",
            topic="events",
            group_id="test_group",
        )
        assert src._brokers == "localhost:9092"
        assert src._topic == "events"
        assert src._group_id == "test_group"

    def test_default_format_is_json(self):
        src = KafkaSource(brokers="b:9092", topic="t", group_id="g")
        assert src._format == "json"

    def test_default_start_from_is_latest(self):
        src = KafkaSource(brokers="b:9092", topic="t", group_id="g")
        assert src._start_from == "latest"

    def test_custom_format(self):
        src = KafkaSource(
            brokers="b:9092", topic="t", group_id="g", format="avro"
        )
        assert src._format == "avro"

    def test_custom_start_from(self):
        src = KafkaSource(
            brokers="b:9092", topic="t", group_id="g", start_from="earliest"
        )
        assert src._start_from == "earliest"

    def test_initial_checkpoint_is_none(self):
        src = KafkaSource(brokers="b:9092", topic="t", group_id="g")
        assert src.checkpoint() is None

    def test_commit_does_not_raise(self):
        src = KafkaSource(brokers="b:9092", topic="t", group_id="g")
        src.commit(42)  # should not raise

    def test_stream_is_iterable(self):
        # We cannot connect to Kafka in unit tests; just verify stream()
        # returns an iterator without immediately failing on a mock con.
        src = KafkaSource(brokers="b:9092", topic="t", group_id="g")
        gen = src.stream(con=None, batch_size=100)
        assert hasattr(gen, "__iter__")
        assert hasattr(gen, "__next__")


# ---------------------------------------------------------------------------
# KinesisSource — constructor and public API (no real AWS)
# ---------------------------------------------------------------------------

class TestKinesisSource:
    def test_basic_construction(self):
        src = KinesisSource(stream_name="my-stream")
        assert src._stream == "my-stream"

    def test_default_region(self):
        src = KinesisSource(stream_name="s")
        assert src._region == "us-east-1"

    def test_default_shard_iterator_type(self):
        src = KinesisSource(stream_name="s")
        assert src._iter_type == "LATEST"

    def test_custom_region(self):
        src = KinesisSource(stream_name="s", region="eu-west-1")
        assert src._region == "eu-west-1"

    def test_custom_shard_iterator_type(self):
        src = KinesisSource(stream_name="s", shard_iterator_type="TRIM_HORIZON")
        assert src._iter_type == "TRIM_HORIZON"

    def test_initial_checkpoint_is_none(self):
        src = KinesisSource(stream_name="s")
        assert src.checkpoint() is None

    def test_commit_does_not_raise(self):
        src = KinesisSource(stream_name="s")
        src.commit("seq-123")  # should not raise

    def test_stream_is_generator_function(self):
        import inspect
        src = KinesisSource(stream_name="s")
        # stream() must return a generator / iterator, not raise on creation
        gen = src.stream(con=None, batch_size=100)
        assert hasattr(gen, "__iter__")


# ---------------------------------------------------------------------------
# DebeziumSource — constructor and delegation (no real CDC)
# ---------------------------------------------------------------------------

class TestDebeziumSource:
    def _make_kafka(self):
        return KafkaSource(brokers="b:9092", topic="t", group_id="g")

    def test_basic_construction(self):
        kafka = self._make_kafka()
        src = DebeziumSource(kafka_source=kafka, natural_key="customer_id")
        assert src._nk == "customer_id"

    def test_stores_kafka_reference(self):
        kafka = self._make_kafka()
        src = DebeziumSource(kafka_source=kafka, natural_key="id")
        assert src._kafka is kafka

    def test_commit_delegates_to_kafka(self):
        committed = []
        kafka = self._make_kafka()
        kafka.commit = lambda offset: committed.append(offset)
        src = DebeziumSource(kafka_source=kafka, natural_key="id")
        src.commit(99)
        assert committed == [99]

    def test_checkpoint_delegates_to_kafka(self):
        kafka = self._make_kafka()
        kafka._offset = 77
        # Patch checkpoint so it returns a known value
        kafka.checkpoint = lambda: 77
        src = DebeziumSource(kafka_source=kafka, natural_key="id")
        assert src.checkpoint() == 77

    def test_stream_wraps_raw_in_upserts_view(self):
        """DebeziumSource.stream() should yield exactly one fragment per
        kafka batch, and that fragment should reference _cdc_upserts."""
        con = duckdb.connect()
        # Seed a fake _cdc_raw table so the SQL doesn't fail
        con.execute("""
            CREATE TABLE _fake_cdc (
                op     VARCHAR,
                after  STRUCT(id INT, name VARCHAR),
                before STRUCT(id INT, name VARCHAR)
            )
        """)
        con.execute("""
            INSERT INTO _fake_cdc VALUES
              ('c', {'id': 1, 'name': 'Alice'}, NULL),
              ('u', {'id': 2, 'name': 'Bob'},   {'id': 2, 'name': 'OldBob'}),
              ('d', NULL,                        {'id': 3, 'name': 'Gone'})
        """)

        class FakeKafka:
            def stream(self, _con, batch_size=10_000):
                yield "SELECT * FROM _fake_cdc"

            def commit(self, offset):
                pass

            def checkpoint(self):
                return 1

        src = DebeziumSource(kafka_source=FakeKafka(), natural_key="id")
        fragments = list(src.stream(con))
        assert len(fragments) == 1
        assert "_cdc_upserts" in fragments[0]

    def test_stream_creates_deleted_nks_view(self):
        """After iterating one batch, a 'deleted_nks' view must exist in
        the connection and contain the natural key of the deleted row."""
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE _cdc_data (
                op     VARCHAR,
                after  STRUCT(id INT, name VARCHAR),
                before STRUCT(id INT, name VARCHAR)
            )
        """)
        con.execute("""
            INSERT INTO _cdc_data VALUES
              ('c', {'id': 1, 'name': 'Alice'}, NULL),
              ('d', NULL,                        {'id': 3, 'name': 'Gone'})
        """)

        class FakeKafka:
            def stream(self, _con, batch_size=10_000):
                yield "SELECT * FROM _cdc_data"

            def commit(self, offset):
                pass

            def checkpoint(self):
                return 1

        src = DebeziumSource(kafka_source=FakeKafka(), natural_key="id")
        list(src.stream(con))  # exhaust iterator

        deleted_ids = con.execute("SELECT id FROM deleted_nks").fetchall()
        assert (3,) in deleted_ids


# ---------------------------------------------------------------------------
# __init__.py re-exports
# ---------------------------------------------------------------------------

class TestSourcesInit:
    def test_stream_source_adapter_importable_from_sources(self):
        from sqldim.sources import StreamSourceAdapter
        assert StreamSourceAdapter is not None

    def test_stream_result_importable_from_sources(self):
        from sqldim.sources import StreamResult
        assert StreamResult is not None

    def test_kafka_source_importable_from_sources(self):
        from sqldim.sources import KafkaSource
        assert KafkaSource is not None

    def test_kinesis_source_importable_from_sources(self):
        from sqldim.sources import KinesisSource
        assert KinesisSource is not None

    def test_debezium_source_importable_from_sources(self):
        from sqldim.sources import DebeziumSource
        assert DebeziumSource is not None
