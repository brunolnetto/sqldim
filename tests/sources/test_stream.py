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
from sqldim.core.kimball.dimensions.scd.handler import SCDResult


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

    def test_multiple_cdc_batches(self):
        """DebeziumSource should process multiple Kafka batches, each
        creating fresh _cdc_upserts and deleted_nks views."""
        con = duckdb.connect()

        # Batch 1 data
        con.execute("""
            CREATE TABLE _batch1 (
                op     VARCHAR,
                after  STRUCT(id INT, name VARCHAR),
                before STRUCT(id INT, name VARCHAR)
            )
        """)
        con.execute("""
            INSERT INTO _batch1 VALUES
              ('c', {'id': 1, 'name': 'Alice'}, NULL),
              ('d', NULL, {'id': 5, 'name': 'Removed'})
        """)

        # Batch 2 data
        con.execute("""
            CREATE TABLE _batch2 (
                op     VARCHAR,
                after  STRUCT(id INT, name VARCHAR),
                before STRUCT(id INT, name VARCHAR)
            )
        """)
        con.execute("""
            INSERT INTO _batch2 VALUES
              ('u', {'id': 1, 'name': 'AliceUpdated'}, {'id': 1, 'name': 'Alice'}),
              ('r', {'id': 2, 'name': 'Snapshot'}, NULL)
        """)

        class FakeKafka:
            def stream(self, _con, batch_size=10_000):
                yield "SELECT * FROM _batch1"
                yield "SELECT * FROM _batch2"

            def commit(self, offset):
                pass

            def checkpoint(self):
                return 2

        src = DebeziumSource(kafka_source=FakeKafka(), natural_key="id")
        fragments = list(src.stream(con))

        assert len(fragments) == 2
        for f in fragments:
            assert "_cdc_upserts" in f

        # Final state of deleted_nks should reflect last batch only
        deleted = con.execute("SELECT id FROM deleted_nks").fetchall()
        assert deleted == []  # batch 2 had no deletes

    def test_all_deletes_batch_yields_empty_upserts(self):
        """When a batch contains only deletes, _cdc_upserts has zero rows
        but the fragment is still yielded."""
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE _del_only (
                op     VARCHAR,
                after  STRUCT(id INT),
                before STRUCT(id INT)
            )
        """)
        con.execute("""
            INSERT INTO _del_only VALUES
              ('d', NULL, {'id': 10}),
              ('d', NULL, {'id': 20})
        """)

        class FakeKafka:
            def stream(self, _con, batch_size=10_000):
                yield "SELECT * FROM _del_only"
            def commit(self, offset): pass
            def checkpoint(self): return 1

        src = DebeziumSource(kafka_source=FakeKafka(), natural_key="id")
        fragments = list(src.stream(con))

        assert len(fragments) == 1
        # The upserts view exists but has zero rows
        count = con.execute("SELECT count(*) FROM _cdc_upserts").fetchone()[0]
        assert count == 0
        # deleted_nks has both keys
        deleted = con.execute("SELECT id FROM deleted_nks ORDER BY id").fetchall()
        assert deleted == [(10,), (20,)]

    def test_snapshot_read_included_in_upserts(self):
        """Debezium 'r' (read/snapshot) rows should appear in _cdc_upserts."""
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE _snap (
                op     VARCHAR,
                after  STRUCT(id INT, val VARCHAR),
                before STRUCT(id INT, val VARCHAR)
            )
        """)
        con.execute("""
            INSERT INTO _snap VALUES
              ('r', {'id': 1, 'val': 'snap1'}, NULL),
              ('r', {'id': 2, 'val': 'snap2'}, NULL),
              ('c', {'id': 3, 'val': 'new'},    NULL)
        """)

        class FakeKafka:
            def stream(self, _con, batch_size=10_000):
                yield "SELECT * FROM _snap"
            def commit(self, offset): pass
            def checkpoint(self): return 1

        src = DebeziumSource(kafka_source=FakeKafka(), natural_key="id")
        list(src.stream(con))

        rows = con.execute(
            "SELECT id, val FROM _cdc_upserts ORDER BY id"
        ).fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, "snap1")
        assert rows[1] == (2, "snap2")
        assert rows[2] == (3, "new")

    def test_update_overwrites_in_upserts_view(self):
        """Multiple ops for the same key in one batch — upserts shows
        the 'after' of each create/update/read, not deduplicated."""
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE _upd (
                op     VARCHAR,
                after  STRUCT(id INT, status VARCHAR),
                before STRUCT(id INT, status VARCHAR)
            )
        """)
        con.execute("""
            INSERT INTO _upd VALUES
              ('c', {'id': 1, 'status': 'new'},      NULL),
              ('u', {'id': 1, 'status': 'updated'},  {'id': 1, 'status': 'new'})
        """)

        class FakeKafka:
            def stream(self, _con, batch_size=10_000):
                yield "SELECT * FROM _upd"
            def commit(self, offset): pass
            def checkpoint(self): return 1

        src = DebeziumSource(kafka_source=FakeKafka(), natural_key="id")
        list(src.stream(con))

        rows = con.execute(
            "SELECT id, status FROM _cdc_upserts ORDER BY id"
        ).fetchall()
        # Both rows appear — the view doesn't deduplicate; that's the
        # caller's responsibility (e.g., via MERGE in the dimension loader).
        assert len(rows) == 2

    def test_commit_and_checkpoint_delegation(self):
        """commit() and checkpoint() pass through to the underlying KafkaSource."""
        call_log = []

        class TrackingKafka:
            def stream(self, _con, batch_size=10_000):
                yield "SELECT 1 AS id"
            def commit(self, offset):
                call_log.append(("commit", offset))
            def checkpoint(self):
                call_log.append(("checkpoint",))
                return 42

        src = DebeziumSource(kafka_source=TrackingKafka(), natural_key="id")
        src.commit(100)
        result = src.checkpoint()

        assert call_log == [("commit", 100), ("checkpoint",)]
        assert result == 42


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


# ---------------------------------------------------------------------------
# KafkaSource._stream_native() — mock DuckDB connection
# ---------------------------------------------------------------------------

class TestKafkaStreamNative:
    def _make_src(self):
        return KafkaSource(brokers="b:9092", topic="t", group_id="g")

    def _mock_con(self, count_seq, offset_seq):
        """Build a MagicMock DuckDB connection that returns count_seq then
        offset_seq in alternating fetchone() calls."""
        from unittest.mock import MagicMock
        con = MagicMock()
        results = []
        for count, offset in zip(count_seq, offset_seq):
            results.append(MagicMock(**{"fetchone.return_value": (count,)}))
            results.append(MagicMock(**{"fetchone.return_value": (offset,)}))
        # Append a final count==0 sentinel to stop the loop
        results.append(MagicMock(**{"fetchone.return_value": (0,)}))
        con.execute.side_effect = results
        return con

    def test_yields_one_sql_fragment_per_non_empty_batch(self):
        from unittest.mock import MagicMock
        src = self._make_src()
        con = self._mock_con(count_seq=[5], offset_seq=[42])
        fragments = list(src._stream_native(con, batch_size=10))
        assert len(fragments) == 1
        assert "kafka_scan" in fragments[0]

    def test_updates_offset_after_each_batch(self):
        src = self._make_src()
        con = self._mock_con(count_seq=[3], offset_seq=[99])
        list(src._stream_native(con, batch_size=10))
        assert src._offset == 99

    def test_stops_when_count_is_zero(self):
        from unittest.mock import MagicMock
        src = self._make_src()
        # First batch has 0 rows → stop immediately
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = (0,)
        fragments = list(src._stream_native(con, batch_size=10))
        assert fragments == []

    def test_yields_multiple_batches(self):
        src = self._make_src()
        con = self._mock_con(count_seq=[5, 3], offset_seq=[10, 20])
        fragments = list(src._stream_native(con, batch_size=10))
        assert len(fragments) == 2
        assert src._offset == 20

    def test_sql_contains_brokers_and_topic(self):
        src = self._make_src()
        con = self._mock_con(count_seq=[1], offset_seq=[0])
        fragment = next(iter(src._stream_native(con, batch_size=1)))
        assert "b:9092" in fragment
        assert "'t'" in fragment


# ---------------------------------------------------------------------------
# KafkaSource._stream_consumer() — mock confluent_kafka + polars
# ---------------------------------------------------------------------------

class TestKafkaStreamConsumer:
    def _make_src(self):
        return KafkaSource(brokers="b:9092", topic="events", group_id="g")

    def _make_mock_msg(self, payload: dict, offset_val: int):
        from unittest.mock import MagicMock
        import json
        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = json.dumps(payload).encode()
        msg.offset.return_value = offset_val
        return msg

    def test_yields_sql_fragment_for_non_empty_batch(self):
        import sys
        from unittest.mock import MagicMock, patch

        msg = self._make_mock_msg({"id": 1, "name": "Alice"}, 7)

        mock_consumer = MagicMock()
        mock_consumer.consume.side_effect = [[msg], []]  # batch then empty

        mock_confluent = MagicMock()
        mock_confluent.Consumer.return_value = mock_consumer

        mock_polars = MagicMock()
        mock_arrow_table = MagicMock()
        mock_polars.from_dicts.return_value.to_arrow.return_value = mock_arrow_table

        con = MagicMock()

        src = self._make_src()
        with patch.dict(sys.modules, {"confluent_kafka": mock_confluent, "polars": mock_polars}):
            fragments = list(src._stream_consumer(con, batch_size=10))

        assert len(fragments) == 1
        assert "_kafka_batch" in fragments[0]

    def test_updates_offset_from_last_message(self):
        import sys
        from unittest.mock import MagicMock, patch

        msg = self._make_mock_msg({"id": 5}, offset_val=99)

        mock_consumer = MagicMock()
        mock_consumer.consume.side_effect = [[msg], []]

        mock_confluent = MagicMock()
        mock_confluent.Consumer.return_value = mock_consumer

        mock_polars = MagicMock()

        src = self._make_src()
        with patch.dict(sys.modules, {"confluent_kafka": mock_confluent, "polars": mock_polars}):
            list(src._stream_consumer(MagicMock(), batch_size=10))

        assert src._offset == 99

    def test_stops_on_empty_consume(self):
        import sys
        from unittest.mock import MagicMock, patch

        mock_consumer = MagicMock()
        mock_consumer.consume.return_value = []  # immediately empty

        mock_confluent = MagicMock()
        mock_confluent.Consumer.return_value = mock_consumer

        src = self._make_src()
        with patch.dict(sys.modules, {"confluent_kafka": mock_confluent, "polars": MagicMock()}):
            fragments = list(src._stream_consumer(MagicMock(), batch_size=10))

        assert fragments == []

    def test_consumer_close_called_in_finally(self):
        import sys
        from unittest.mock import MagicMock, patch

        mock_consumer = MagicMock()
        mock_consumer.consume.return_value = []

        mock_confluent = MagicMock()
        mock_confluent.Consumer.return_value = mock_consumer

        src = self._make_src()
        with patch.dict(sys.modules, {"confluent_kafka": mock_confluent, "polars": MagicMock()}):
            list(src._stream_consumer(MagicMock(), batch_size=10))

        mock_consumer.close.assert_called_once()

    def test_skips_messages_with_errors(self):
        """Messages where m.error() is not None should be skipped."""
        import sys
        import json
        from unittest.mock import MagicMock, patch

        # One error message, one good message
        bad_msg = MagicMock()
        bad_msg.error.return_value = MagicMock()  # truthy error
        bad_msg.value.return_value = b'{"id": 99}'
        bad_msg.offset.return_value = 1

        good_msg = self._make_mock_msg({"id": 1}, offset_val=2)

        mock_consumer = MagicMock()
        mock_consumer.consume.side_effect = [[bad_msg, good_msg], []]

        mock_confluent = MagicMock()
        mock_confluent.Consumer.return_value = mock_consumer

        mock_polars = MagicMock()

        src = self._make_src()
        with patch.dict(sys.modules, {"confluent_kafka": mock_confluent, "polars": mock_polars}):
            list(src._stream_consumer(MagicMock(), batch_size=10))

        # from_dicts should only have been called with the good message's data
        call_args = mock_polars.from_dicts.call_args[0][0]
        assert call_args == [{"id": 1}]


# ---------------------------------------------------------------------------
# KafkaSource.stream() — try/except: native → consumer fallback
# ---------------------------------------------------------------------------

class TestKafkaStream:
    def test_stream_falls_back_to_consumer_when_load_fails(self):
        """If 'LOAD kafka;' raises, stream() delegates to _stream_consumer."""
        import sys
        from unittest.mock import MagicMock, patch
        import json

        msg_data = {"id": 1}
        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = json.dumps(msg_data).encode()
        msg.offset.return_value = 5

        mock_consumer = MagicMock()
        mock_consumer.consume.side_effect = [[msg], []]
        mock_confluent = MagicMock()
        mock_confluent.Consumer.return_value = mock_consumer

        mock_polars = MagicMock()

        # MagicMock connection — execute("LOAD kafka;") raises to trigger fallback
        con = MagicMock()
        con.execute.side_effect = Exception("kafka extension not found")

        src = KafkaSource(brokers="b:9092", topic="events", group_id="g")
        with patch.dict(sys.modules, {"confluent_kafka": mock_confluent, "polars": mock_polars}):
            fragments = list(src.stream(con, batch_size=10))

        assert len(fragments) == 1
        assert "_kafka_batch" in fragments[0]


# ---------------------------------------------------------------------------
# KinesisSource.stream() — mock boto3 + polars
# ---------------------------------------------------------------------------

class TestKinesisStream:
    def _make_src(self):
        return KinesisSource(stream_name="my-stream", region="us-east-1")

    def test_yields_one_fragment_per_shard_batch(self):
        import sys
        import json
        from unittest.mock import MagicMock, patch

        record = {"Data": json.dumps({"id": 1}).encode(), "SequenceNumber": "seq-1"}

        mock_client = MagicMock()
        mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shard-000"}]}
        mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-0"}
        mock_client.get_records.side_effect = [
            {"Records": [record], "NextShardIterator": "iter-1"},
            {"Records": [], "NextShardIterator": None},
        ]

        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_client

        mock_polars = MagicMock()

        con = MagicMock()
        src = self._make_src()
        with patch.dict(sys.modules, {"boto3": mock_boto3, "polars": mock_polars}):
            fragments = list(src.stream(con, batch_size=100))

        assert len(fragments) == 1
        assert "_kinesis_batch" in fragments[0]

    def test_updates_sequence_number_after_batch(self):
        import sys
        import json
        from unittest.mock import MagicMock, patch

        record = {"Data": json.dumps({"x": 1}).encode(), "SequenceNumber": "seq-42"}

        mock_client = MagicMock()
        mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shard-000"}]}
        mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-0"}
        mock_client.get_records.side_effect = [
            {"Records": [record], "NextShardIterator": None},
        ]

        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_client

        src = self._make_src()
        with patch.dict(sys.modules, {"boto3": mock_boto3, "polars": MagicMock()}):
            list(src.stream(MagicMock(), batch_size=100))

        assert src._seq == "seq-42"

    def test_stops_when_no_records(self):
        import sys
        from unittest.mock import MagicMock, patch

        mock_client = MagicMock()
        mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shard-000"}]}
        mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-0"}
        mock_client.get_records.return_value = {"Records": [], "NextShardIterator": "iter-1"}

        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_client

        src = self._make_src()
        with patch.dict(sys.modules, {"boto3": mock_boto3, "polars": MagicMock()}):
            fragments = list(src.stream(MagicMock(), batch_size=100))

        assert fragments == []

    def test_iterates_multiple_shards(self):
        import sys
        import json
        from unittest.mock import MagicMock, patch

        r1 = {"Data": json.dumps({"id": 1}).encode(), "SequenceNumber": "s1"}
        r2 = {"Data": json.dumps({"id": 2}).encode(), "SequenceNumber": "s2"}

        mock_client = MagicMock()
        mock_client.list_shards.return_value = {
            "Shards": [{"ShardId": "shard-000"}, {"ShardId": "shard-001"}]
        }
        mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-X"}
        mock_client.get_records.side_effect = [
            {"Records": [r1], "NextShardIterator": None},
            {"Records": [r2], "NextShardIterator": None},
        ]

        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_client

        src = self._make_src()
        with patch.dict(sys.modules, {"boto3": mock_boto3, "polars": MagicMock()}):
            fragments = list(src.stream(MagicMock(), batch_size=100))

        assert len(fragments) == 2
        assert src._seq == "s2"

    def test_multiple_batches_from_single_shard(self):
        """A single shard can yield multiple batches before NextShardIterator is None."""
        import sys
        import json
        from unittest.mock import MagicMock, patch

        r1 = {"Data": json.dumps({"id": 1}).encode(), "SequenceNumber": "s1"}
        r2 = {"Data": json.dumps({"id": 2}).encode(), "SequenceNumber": "s2"}

        mock_client = MagicMock()
        mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shard-000"}]}
        mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-0"}
        mock_client.get_records.side_effect = [
            {"Records": [r1], "NextShardIterator": "iter-1"},
            {"Records": [r2], "NextShardIterator": None},
        ]

        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_client

        src = self._make_src()
        with patch.dict(sys.modules, {"boto3": mock_boto3, "polars": MagicMock()}):
            fragments = list(src.stream(MagicMock(), batch_size=100))

        assert len(fragments) == 2
        assert src._seq == "s2"

    def test_empty_shards_list_yields_nothing(self):
        """When the stream has zero shards, stream() yields nothing."""
        import sys
        from unittest.mock import MagicMock, patch

        mock_client = MagicMock()
        mock_client.list_shards.return_value = {"Shards": []}

        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_client

        src = self._make_src()
        with patch.dict(sys.modules, {"boto3": mock_boto3, "polars": MagicMock()}):
            fragments = list(src.stream(MagicMock(), batch_size=100))

        assert fragments == []
        mock_client.get_shard_iterator.assert_not_called()

    def test_batch_size_passed_to_get_records(self):
        """The batch_size parameter must be forwarded to get_records(Limit=...)."""
        import sys
        import json
        from unittest.mock import MagicMock, patch

        r = {"Data": json.dumps({"x": 1}).encode(), "SequenceNumber": "s"}
        mock_client = MagicMock()
        mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shard-000"}]}
        mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-0"}
        mock_client.get_records.side_effect = [
            {"Records": [r], "NextShardIterator": None},
        ]

        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_client

        src = self._make_src()
        with patch.dict(sys.modules, {"boto3": mock_boto3, "polars": MagicMock()}):
            list(src.stream(MagicMock(), batch_size=500))

        mock_client.get_records.assert_called_once_with(
            ShardIterator="iter-0", Limit=500
        )

    def test_registers_arrow_table_as_kinesis_batch(self):
        """stream() must call con.register('_kinesis_batch', arrow_table)."""
        import sys
        import json
        from unittest.mock import MagicMock, patch

        r = {"Data": json.dumps({"id": 1}).encode(), "SequenceNumber": "s"}
        mock_client = MagicMock()
        mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shard-000"}]}
        mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-0"}
        mock_client.get_records.side_effect = [
            {"Records": [r], "NextShardIterator": None},
        ]

        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_client

        fake_arrow = MagicMock()
        mock_polars = MagicMock()
        mock_polars.from_dicts.return_value.to_arrow.return_value = fake_arrow

        con = MagicMock()
        src = self._make_src()
        with patch.dict(sys.modules, {"boto3": mock_boto3, "polars": mock_polars}):
            list(src.stream(con, batch_size=100))

        con.register.assert_called_once_with("_kinesis_batch", fake_arrow)

    def test_stops_when_next_shard_iterator_is_none(self):
        """When NextShardIterator is falsy (None/''), the while loop stops."""
        import sys
        import json
        from unittest.mock import MagicMock, patch

        r = {"Data": json.dumps({"x": 1}).encode(), "SequenceNumber": "s"}
        mock_client = MagicMock()
        mock_client.list_shards.return_value = {"Shards": [{"ShardId": "shard-000"}]}
        mock_client.get_shard_iterator.return_value = {"ShardIterator": "iter-0"}
        # Return records then NextShardIterator=None → loop stops
        mock_client.get_records.side_effect = [
            {"Records": [r], "NextShardIterator": None},
        ]

        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_client

        src = self._make_src()
        with patch.dict(sys.modules, {"boto3": mock_boto3, "polars": MagicMock()}):
            fragments = list(src.stream(MagicMock(), batch_size=100))

        assert len(fragments) == 1


# ---------------------------------------------------------------------------
# KafkaSource fallback path  (kafka.py lines 70, 140)
# ---------------------------------------------------------------------------

class TestKafkaSourceFallback:
    """Covers lines 70 (consumer fallback) and 140 (yield in _stream_consumer)."""

    def _make_src(self):
        return KafkaSource(
            brokers="localhost:9092",
            topic="test_topic",
            group_id="test_group",
        )

    def test_stream_falls_back_to_consumer_when_kafka_extension_missing(self):
        """When 'LOAD kafka' raises, stream() falls back to _stream_consumer (line 70)."""
        from unittest.mock import MagicMock, patch

        src = self._make_src()
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("Extension 'kafka' not found")

        # Stub _stream_consumer so we don't need the real confluent-kafka library.
        def _stub_consumer(con, batch_size):
            yield "SELECT 1 AS id"

        with patch.object(src, "_stream_consumer", _stub_consumer):
            fragments = list(src.stream(mock_con, batch_size=100))

        assert fragments == ["SELECT 1 AS id"]

    def test_stream_uses_native_path_when_kafka_loads(self):
        """When 'LOAD kafka' succeeds, stream() uses _stream_native (line 74)."""
        from unittest.mock import MagicMock, patch

        src = self._make_src()
        mock_con = MagicMock()
        # execute("LOAD kafka;") does not raise → native = True
        mock_con.execute.return_value = MagicMock()

        def _stub_native(con, batch_size):
            yield "NATIVE_FRAG"

        with patch.object(src, "_stream_native", _stub_native):
            fragments = list(src.stream(mock_con, batch_size=100))

        assert fragments == ["NATIVE_FRAG"]

    def test_stream_consumer_yields_batch_fragment(self):
        """_stream_consumer yields 'SELECT * FROM _kafka_batch' for each message
        batch (line 140), and updates self._offset to the last message offset."""
        import json
        import sys
        from unittest.mock import MagicMock, patch

        src = self._make_src()

        # Build a fake Kafka message.
        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = json.dumps({"id": 1, "name": "test"}).encode()
        msg.offset.return_value = 42

        mock_consumer = MagicMock()
        # First consume() returns [msg], second returns [] → loop stops.
        mock_consumer.consume.side_effect = [[msg], []]

        mock_confluent = MagicMock()
        mock_confluent.Consumer.return_value = mock_consumer

        mock_df = MagicMock()
        mock_df.to_arrow.return_value = "arrow_table"
        mock_polars = MagicMock()
        mock_polars.from_dicts.return_value = mock_df

        mock_con = MagicMock()

        with patch.dict(sys.modules, {
            "confluent_kafka": mock_confluent,
            "polars": mock_polars,
        }):
            fragments = list(src._stream_consumer(mock_con, batch_size=100))

        assert fragments == ["SELECT * FROM _kafka_batch"]
        assert src._offset == 42
        mock_con.register.assert_called_once_with("_kafka_batch", "arrow_table")

    def test_stream_consumer_skips_error_messages(self):
        """When all messages in a poll have errors, rows is empty and the
        'continue' branch at line 140 is taken without yielding a fragment."""
        import json
        import sys
        from unittest.mock import MagicMock, patch

        src = self._make_src()

        bad_msg = MagicMock()
        bad_msg.error.return_value = "kafka-error"   # truthy → filtered out

        good_msg = MagicMock()
        good_msg.error.return_value = None
        good_msg.value.return_value = json.dumps({"id": 2}).encode()
        good_msg.offset.return_value = 7

        mock_consumer = MagicMock()
        # Batch 1: only error message → rows=[] → continue (line 140)
        # Batch 2: good message → yields fragment
        # Batch 3: empty → loop breaks
        mock_consumer.consume.side_effect = [[bad_msg], [good_msg], []]

        mock_confluent = MagicMock()
        mock_confluent.Consumer.return_value = mock_consumer

        mock_df = MagicMock()
        mock_df.to_arrow.return_value = "arrow2"
        mock_polars = MagicMock()
        mock_polars.from_dicts.return_value = mock_df

        mock_con = MagicMock()

        with patch.dict(sys.modules, {
            "confluent_kafka": mock_confluent,
            "polars": mock_polars,
        }):
            fragments = list(src._stream_consumer(mock_con, batch_size=100))

        # Only the good batch yielded a fragment; error batch hit continue
        assert fragments == ["SELECT * FROM _kafka_batch"]
        assert src._offset == 7
