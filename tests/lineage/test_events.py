"""Tests for lineage event model — LineageEvent, RunState, DatasetRef."""
from datetime import datetime, timezone

from sqldim.lineage import LineageEvent, RunState
from sqldim.lineage.events import DatasetRef


# ---------------------------------------------------------------------------
# RunState
# ---------------------------------------------------------------------------

class TestRunState:
    def test_all_states_exist(self):
        assert RunState.START
        assert RunState.RUNNING
        assert RunState.COMPLETE
        assert RunState.FAIL
        assert RunState.ABORT

    def test_distinct(self):
        states = list(RunState)
        assert len(states) == len(set(states))

    def test_string_values(self):
        assert RunState.START.value == "START"
        assert RunState.COMPLETE.value == "COMPLETE"
        assert RunState.FAIL.value == "FAIL"


# ---------------------------------------------------------------------------
# DatasetRef
# ---------------------------------------------------------------------------

class TestDatasetRef:
    def test_basic_fields(self):
        d = DatasetRef(namespace="sqldim.bronze", name="raw_orders")
        assert d.namespace == "sqldim.bronze"
        assert d.name == "raw_orders"
        assert d.facets == {}

    def test_with_facets(self):
        d = DatasetRef(
            namespace="sqldim.silver",
            name="dim_customer",
            facets={"rows": 1000, "schema": "..."},
        )
        assert d.facets["rows"] == 1000


# ---------------------------------------------------------------------------
# LineageEvent
# ---------------------------------------------------------------------------

class TestLineageEvent:
    def test_defaults(self):
        ev = LineageEvent(job_name="test_job")
        assert ev.run_id  # auto-generated UUID hex
        assert len(ev.run_id) == 32
        assert ev.job_name == "test_job"
        assert ev.namespace == "sqldim"
        assert ev.state is RunState.START
        assert ev.inputs == []
        assert ev.outputs == []
        assert ev.facets == {}
        assert ev.event_time is not None

    def test_custom_run_id(self):
        ev = LineageEvent(run_id="abc123", job_name="x")
        assert ev.run_id == "abc123"

    def test_with_inputs_and_outputs(self):
        ev = LineageEvent(
            job_name="load.dim_customer",
            state=RunState.COMPLETE,
            inputs=[DatasetRef("sqldim.bronze", "raw_customers")],
            outputs=[DatasetRef("sqldim.silver", "dim_customer")],
        )
        assert len(ev.inputs) == 1
        assert ev.inputs[0].name == "raw_customers"
        assert len(ev.outputs) == 1
        assert ev.outputs[0].name == "dim_customer"

    def test_to_dict_shape(self):
        ev = LineageEvent(
            run_id="run1",
            job_name="load.fact_orders",
            state=RunState.COMPLETE,
            inputs=[DatasetRef("sqldim.bronze", "raw_orders")],
            outputs=[DatasetRef("sqldim.silver", "fact_orders")],
        )
        d = ev.to_dict()
        assert d["eventType"] == "COMPLETE"
        assert d["eventTime"] is not None
        assert d["run"]["runId"] == "run1"
        assert d["job"]["namespace"] == "sqldim"
        assert d["job"]["name"] == "load.fact_orders"
        assert len(d["inputs"]) == 1
        assert d["inputs"][0]["name"] == "raw_orders"
        assert len(d["outputs"]) == 1
        assert d["outputs"][0]["name"] == "fact_orders"

    def test_to_dict_with_facets(self):
        ev = LineageEvent(
            job_name="x",
            facets={"rows_loaded": 500},
        )
        d = ev.to_dict()
        assert d["run"]["facets"]["rows_loaded"] == 500

    def test_to_dict_empty_inputs_outputs(self):
        ev = LineageEvent(job_name="x")
        d = ev.to_dict()
        assert d["inputs"] == []
        assert d["outputs"] == []

    def test_to_dict_is_json_serialisable(self):
        import json
        ev = LineageEvent(
            job_name="x",
            inputs=[DatasetRef("ns", "tbl", facets={"count": 10})],
        )
        json_str = json.dumps(ev.to_dict())
        assert '"eventType": "START"' in json_str

    def test_event_time_is_recent(self):
        before = datetime.now(timezone.utc)
        ev = LineageEvent(job_name="x")
        after = datetime.now(timezone.utc)
        assert before <= ev.event_time <= after

    def test_all_run_states_produce_valid_dicts(self):
        import json
        for state in RunState:
            ev = LineageEvent(job_name="x", state=state)
            d = ev.to_dict()
            assert d["eventType"] == state.value
            json.dumps(d)  # must not raise
