"""§12.3 dgm_validate_invariant: complete evaluation or a typed failure (D43)."""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb
import pytest

from sqldim.core.query.dgm.annotations import SCDKind
from sqldim.core.query.dgm.nl.supervisor._auth import Action
from sqldim.core.query.dgm.nl.supervisor._dispatch import execute_tool
from sqldim.core.query.dgm.nl.supervisor._invariant_core import NotEvaluable
from sqldim.core.query.dgm.nl.supervisor._invariant_model import (
    DGMModel,
    EdgeKind,
    EdgeType,
    ForeignKey,
    Holder,
    NodeKind,
    NodeTable,
    NodeType,
    PipelineArtifactRef,
    PropType,
    Reference,
    RelationTable,
)
from sqldim.core.query.dgm.nl.supervisor._schemas import (
    INVARIANTS,
    ValidateInvariantOutput,
)
from sqldim.core.query.dgm.nl.supervisor._tools import build_registry
from sqldim.core.query.dgm.nl.supervisor._tools_invariants import (
    CostNotEstimated,
    ValidateInvariantTool,
    evaluate_invariant,
)
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    CancelToken,
    ToolCallResult,
    ToolErrorKind,
)
from sqldim.core.query.dgm.nl.supervisor._validation import bind_plan
from tests.supervisor._fakes import auth, plan, step, tool_context

# ---------------------------------------------------------------------------
# Fixture dataset: a retail model with an SCD2 customer (§2.6 example)
# ---------------------------------------------------------------------------

RETAIL_SQL = [
    "CREATE TABLE dim_customer(version_key INTEGER, customer_id VARCHAR, "
    "segment VARCHAR, effective_from DATE, effective_to DATE)",
    "INSERT INTO dim_customer VALUES "
    "(101, 'C1', 'retail', '2023-01-01', '2024-03-01'), "
    "(102, 'C1', 'wholesale', '2024-03-01', NULL), "
    "(201, 'C2', 'retail', '2023-01-01', NULL)",
    "CREATE TABLE dim_product(product_id VARCHAR, name VARCHAR, price DOUBLE)",
    "INSERT INTO dim_product VALUES ('P1', 'pen', 1.5), ('P2', 'ink', 3.0)",
    "CREATE TABLE fact_sale(sale_id VARCHAR, customer_id VARCHAR, "
    "product_id VARCHAR, occurred_on DATE, revenue DOUBLE)",
    # S3 references no customer: it yields no 'placed' edge (M5).
    "INSERT INTO fact_sale VALUES ('S1', 'C1', 'P1', '2023-06-10', 600), "
    "('S2', 'C2', 'P2', '2024-01-05', 20), ('S3', 'C9', 'P1', '2024-02-01', 5)",
    "CREATE TABLE bridge_refers(source_id VARCHAR, target_id VARCHAR, weight DOUBLE)",
    "INSERT INTO bridge_refers VALUES ('C1', 'C2', 0.5)",
]

CUSTOMER = NodeType(
    "Customer",
    NodeKind.DIM,
    {"segment": PropType.enum("retail", "wholesale")},
    NodeTable(
        "dim_customer",
        ("customer_id",),
        {"segment": "segment"},
        version_key="version_key",
        effective_from="effective_from",
        effective_to="effective_to",
    ),
    scd=SCDKind.SCD2,
)
PRODUCT = NodeType(
    "Product",
    NodeKind.DIM,
    {"name": PropType("String"), "price": PropType("Real")},
    NodeTable("dim_product", ("product_id",), {"name": "name", "price": "price"}),
)
SALE = NodeType(
    "Sale",
    NodeKind.FACT,
    {"revenue": PropType("Real"), "occurred_on": PropType("Date")},
    NodeTable(
        "fact_sale",
        ("sale_id",),
        {"revenue": "revenue", "occurred_on": "occurred_on"},
        event_time="occurred_on",
    ),
)
PLACED = EdgeType(
    "Customer",
    "Sale",
    "placed",
    EdgeKind.VERB,
    mapping=ForeignKey(Holder.TARGET, ("customer_id",), Reference.ENTITY_KEY),
)
SOLD = EdgeType(
    "Product",
    "Sale",
    "sold",
    EdgeKind.VERB,
    mapping=ForeignKey(Holder.TARGET, ("product_id",), Reference.ENTITY_KEY),
)
REFERS = EdgeType(
    "Customer",
    "Customer",
    "refers",
    EdgeKind.BRIDGE,
    {"weight": PropType("Real")},
    weighted=True,
    mapping=RelationTable(
        "bridge_refers",
        ("source_id",),
        ("target_id",),
        Reference.ENTITY_KEY,
        {"weight": "weight"},
    ),
)
RETAIL = DGMModel((CUSTOMER, PRODUCT, SALE), (PLACED, SOLD, REFERS))

# Bags: 3 customer versions + 2 products + 3 sales; edges: placed 2 (S3 is
# dangling), sold 3, refers 1.
RETAIL_BAGS = 8 + 6

# ---------------------------------------------------------------------------
# Fixture dataset: a PipelineArtifact (§2.4)
# ---------------------------------------------------------------------------

PIPELINE_SQL = [
    "CREATE TABLE artifact(run_id VARCHAR, state VARCHAR)",
    "INSERT INTO artifact VALUES ('R1', 'Complete'), ('R2', 'Missing')",
    "CREATE TABLE transitions(tid VARCHAR, run_id VARCHAR, prev_tid VARCHAR, "
    "from_state VARCHAR, kind VARCHAR, to_state VARCHAR, occurred_at TIMESTAMP)",
    "INSERT INTO transitions VALUES "
    "('T1', 'R1', NULL, 'Missing', 'scheduled', 'In-flight', '2024-01-01 10:00'), "
    "('T2', 'R1', 'T1', 'In-flight', 'succeeded', 'Complete', '2024-01-01 11:00')",
]

RUN = NodeType(
    "Run",
    NodeKind.FACT,
    {"state": PropType.enum("Missing", "In-flight", "Complete", "Stale", "Failed")},
    NodeTable("artifact", ("run_id",), {"state": "state"}),
)
TRANSITION = NodeType(
    "RunTransition",
    NodeKind.FACT,
    {},
    NodeTable(
        "transitions",
        ("tid",),
        {
            "from_state": "from_state",
            "kind": "kind",
            "to_state": "to_state",
            "occurred_at": "occurred_at",
        },
    ),
)
TRANSITION_OF = EdgeType(
    "RunTransition",
    "Run",
    "transition_of",
    EdgeKind.BRIDGE,
    mapping=ForeignKey(Holder.SOURCE, ("run_id",), Reference.ENTITY_KEY),
)
FOLLOWED_BY = EdgeType(
    "RunTransition",
    "RunTransition",
    "followed_by",
    EdgeKind.BRIDGE,
    mapping=ForeignKey(Holder.TARGET, ("prev_tid",), Reference.ENTITY_KEY),
)
PIPELINE = DGMModel(
    (RUN, TRANSITION),
    (TRANSITION_OF, FOLLOWED_BY),
    (PipelineArtifactRef("Run", "RunTransition"),),
)


class DuckSource:
    """A PipelineSource over in-memory DuckDB that declares its DGM model."""

    def __init__(self, model: DGMModel, sql: list[str]) -> None:
        self._model = model
        self._sql = sql
        self.con: duckdb.DuckDBPyConnection | None = None
        self.setups = 0
        self.teardowns = 0

    def dgm_model(self) -> DGMModel:
        return self._model

    def setup(self) -> None:
        self.setups += 1
        self.con = duckdb.connect(":memory:")
        for statement in self._sql:
            self.con.execute(statement)

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        assert self.con is not None
        return self.con

    def get_table_names(self) -> list[str]:
        return []

    def teardown(self) -> None:
        self.teardowns += 1
        if self.con is not None:
            self.con.close()
            self.con = None


class NoModelSource(DuckSource):
    """A dataset handle that declares no DGM model."""

    dgm_model = None  # type: ignore[assignment]


class SourceOpener:
    def __init__(self, source: Any) -> None:
        self.source = source
        self.opened: list[str] = []

    def __call__(self, dataset_id: str) -> Any:
        self.opened.append(dataset_id)
        return self.source


def _validate(
    invariant: str,
    source: Any,
    *,
    max_witnesses: int = 10,
    dataset: str = "sales",
    grants: dict[str, set[Action]] | None = None,
) -> tuple[ToolCallResult, SourceOpener]:
    request = step(
        "v",
        "dgm_validate_invariant",
        {"dataset": dataset, "invariant": invariant, "max_witnesses": max_witnesses},
    )
    bound, errors = bind_plan(plan(request))
    assert errors == []
    opener = SourceOpener(source)
    ctx = tool_context(
        auth(grants),
        opener=opener,  # type: ignore[arg-type]
        deadline=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    # Generous wall time: these tests check results, not the deadline.
    ctx = dataclasses.replace(
        ctx, budget=dataclasses.replace(ctx.budget, max_wall_time=timedelta(minutes=5))
    )
    result = asyncio.run(execute_tool(bound.steps[0], build_registry(), ctx))
    return result, opener


def _holds(invariant: str, source: Any, **kw: Any) -> dict[str, Any]:
    result, _ = _validate(invariant, source, **kw)
    assert result.status is CallStatus.SUCCEEDED, result.error
    assert result.is_contract_success
    ValidateInvariantOutput.model_validate(result.data)
    return dict(result.data)


def _retail(*extra: str, model: DGMModel = RETAIL) -> DuckSource:
    return DuckSource(model, RETAIL_SQL + list(extra))


def _pipeline(*extra: str, model: DGMModel = PIPELINE) -> DuckSource:
    return DuckSource(model, PIPELINE_SQL + list(extra))


def _failed(invariant: str, source: Any, kind: ToolErrorKind) -> ToolCallResult:
    result, _ = _validate(invariant, source)
    assert result.status is CallStatus.FAILED
    assert result.error is not None and result.error.kind is kind, result.error
    assert result.data is None
    return result


# ---------------------------------------------------------------------------
# Registration and refusals
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_contract_tool_is_available_without_demo(self) -> None:
        schema = build_registry()["dgm_validate_invariant"].capability_schema
        assert schema.available is True
        assert "+demo" not in schema.version

    def test_sweep_stays_unavailable_and_names_the_dependency(self) -> None:
        tool = build_registry()["dgm_sensitivity_sweep"]
        assert tool.capability_schema.available is False
        assert "#153" in getattr(tool, "missing")

    def test_cost_is_never_guessed(self) -> None:
        with pytest.raises(CostNotEstimated):
            ValidateInvariantTool().cost_estimate({}, tool_context())


class TestRefusals:
    def test_ungranted_dataset_reads_nothing(self) -> None:
        source = _retail()
        result, opener = _validate("H3", source, dataset="secret")
        assert result.status is CallStatus.FAILED
        assert result.error is not None
        assert result.error.kind is ToolErrorKind.UNAUTHORIZED
        assert opener.opened == [] and source.setups == 0

    def test_unknown_invariant_is_invalid_arguments(self) -> None:
        result, opener = _validate("commit_count_positive", _retail())
        assert result.error is not None
        assert result.error.kind is ToolErrorKind.INVALID_ARGUMENTS
        assert opener.opened == []

    def test_witness_bound_above_policy_is_invalid_not_truncated(self) -> None:
        result, _ = _validate("H3", _retail(), max_witnesses=10_000)
        assert result.error is not None
        assert result.error.kind is ToolErrorKind.INVALID_ARGUMENTS

    def test_dataset_without_declared_model_is_unsupported(self) -> None:
        source = NoModelSource(RETAIL, RETAIL_SQL)
        result = _failed("H3", source, ToolErrorKind.UNSUPPORTED)
        assert "declares no DGM model" in result.error.message  # type: ignore[union-attr]
        assert source.setups == 0

    @pytest.mark.parametrize("invariant", ["H1", "M5"])
    def test_invariant_true_by_construction_is_unsupported(
        self, invariant: str
    ) -> None:
        _failed(invariant, _retail(), ToolErrorKind.UNSUPPORTED)

    @pytest.mark.parametrize("invariant", ["PA1", "PA2", "PA3", "PA4"])
    def test_pa_without_pipeline_artifact_is_invalid_arguments(
        self, invariant: str
    ) -> None:
        _failed(invariant, _retail(), ToolErrorKind.INVALID_ARGUMENTS)

    def test_pa3_is_a_definition_and_unsupported(self) -> None:
        _failed("PA3", _pipeline(), ToolErrorKind.UNSUPPORTED)

    def test_every_invariant_has_a_typed_end(self) -> None:
        for invariant in INVARIANTS:
            result, _ = _validate(invariant, _retail())
            assert result.status in (CallStatus.SUCCEEDED, CallStatus.FAILED)
            if result.status is CallStatus.FAILED:
                assert result.error is not None and result.data is None


# ---------------------------------------------------------------------------
# Property bags: H2, H3
# ---------------------------------------------------------------------------


class TestPropertyBags:
    def test_h3_holds_and_counts_every_bag(self) -> None:
        data = _holds("H3", _retail())
        assert data == {
            "holds": True,
            "checked": RETAIL_BAGS,
            "violation_count": 0,
            "violations": [],
        }

    def test_h3_enum_violation_has_a_witness(self) -> None:
        data = _holds(
            "H3",
            _retail(
                "INSERT INTO dim_customer VALUES (301, 'C3', 'vip', '2023-01-01', NULL)"
            ),
        )
        assert data["holds"] is False
        assert data["checked"] == RETAIL_BAGS + 1
        assert data["violation_count"] == 1
        [witness] = data["violations"]
        assert witness["node_type"] == "Customer"
        assert witness["instance"] == ["C3"]
        assert witness["values"]["segment"]["value"] == "vip"

    def test_h3_column_type_that_does_not_conform(self) -> None:
        model = dataclasses.replace(
            RETAIL,
            node_types=(
                CUSTOMER,
                dataclasses.replace(
                    PRODUCT,
                    properties={"name": PropType("Integer"), "price": PropType("Real")},
                ),
                SALE,
            ),
        )
        data = _holds("H3", _retail(model=model))
        assert data["violation_count"] == 2  # both product names are not Integers
        assert {w["values"]["name"]["column_type"] for w in data["violations"]} == {
            "VARCHAR"
        }

    def test_witnesses_are_truncated_but_the_count_is_full(self) -> None:
        bad = [
            f"INSERT INTO dim_customer VALUES ({400 + i}, 'X{i}', 'vip', '2023-01-01', NULL)"
            for i in range(5)
        ]
        data = _holds("H3", _retail(*bad), max_witnesses=2)
        assert data["violation_count"] == 5
        assert len(data["violations"]) == 2
        data = _holds("H3", _retail(*bad), max_witnesses=0)
        assert data["violation_count"] == 5 and data["violations"] == []
        assert data["holds"] is False

    def test_h2_holds(self) -> None:
        assert _holds("H2", _retail())["checked"] == RETAIL_BAGS

    def test_h2_undeclared_mapped_property(self) -> None:
        mapping = dataclasses.replace(
            PRODUCT.mapping,  # type: ignore[arg-type]
            props={"name": "name", "price": "price", "colour": "name"},
        )
        model = dataclasses.replace(
            RETAIL,
            node_types=(CUSTOMER, dataclasses.replace(PRODUCT, mapping=mapping), SALE),
        )
        data = _holds("H2", _retail(model=model))
        assert data["violation_count"] == 2
        assert data["violations"][0]["undeclared"] == ["colour"]

    def test_unmapped_type_makes_bag_invariants_unsupported(self) -> None:
        model = dataclasses.replace(
            RETAIL,
            node_types=(CUSTOMER, dataclasses.replace(PRODUCT, mapping=None), SALE),
        )
        _failed("H3", _retail(model=model), ToolErrorKind.UNSUPPORTED)


# ---------------------------------------------------------------------------
# Schema-level: PARTITION, M1, M4
# ---------------------------------------------------------------------------


class TestSchemaLevel:
    def test_partition_holds(self) -> None:
        source = _retail()
        data = _holds("PARTITION", source)
        assert data["checked"] == 6 and data["holds"] is True
        assert source.setups == 0  # decided from the declared model alone

    def test_partition_verb_edge_from_a_fact(self) -> None:
        bad = EdgeType("Sale", "Product", "returns", EdgeKind.VERB)
        model = dataclasses.replace(RETAIL, edge_types=(PLACED, SOLD, REFERS, bad))
        data = _holds("PARTITION", _retail(model=model))
        assert data["violation_count"] == 1
        assert data["violations"][0]["edge"] == bad.key

    def test_m1_holds_and_is_violated(self) -> None:
        assert _holds("M1", _retail())["checked"] == 6
        model = dataclasses.replace(
            RETAIL, edge_types=(PLACED, dataclasses.replace(SOLD, mapping=None), REFERS)
        )
        data = _holds("M1", _retail(model=model))
        assert data["violation_count"] == 1

    def test_m4_weighted_edge_mapped_by_foreign_key(self) -> None:
        assert _holds("M4", _retail())["holds"] is True
        weighted = dataclasses.replace(SOLD, weighted=True)
        model = dataclasses.replace(RETAIL, edge_types=(PLACED, weighted, REFERS))
        data = _holds("M4", _retail(model=model))
        assert data["violations"] == [
            {
                "edge": weighted.key,
                "mapping": "ForeignKey",
                "because": ["declares a weight"],
            }
        ]


# ---------------------------------------------------------------------------
# M2, M7
# ---------------------------------------------------------------------------


class TestVersioning:
    def test_m2_holds(self) -> None:
        data = _holds("M2", _retail())
        assert data == {
            "holds": True,
            "checked": 3 + 3,
            "violation_count": 0,
            "violations": [],
        }

    def test_m2_overlap_null_and_inverted_intervals(self) -> None:
        data = _holds(
            "M2",
            _retail(
                "INSERT INTO dim_customer VALUES (103, 'C1', 'retail', '2024-06-01', NULL)",
                "INSERT INTO dim_customer VALUES (301, 'C3', 'retail', NULL, NULL)",
                "INSERT INTO dim_customer VALUES (401, 'C4', 'retail', '2024-01-01', '2023-01-01')",
            ),
        )
        problems = sorted(w["problem"] for w in data["violations"])
        # 102 and 103 overlap each other; C3 has no start; C4 ends before it starts.
        assert data["violation_count"] == 4
        assert problems == [
            "effective_from is NULL",
            "effective_to <= effective_from",
            "overlaps another version of the same entity_key",
            "overlaps another version of the same entity_key",
        ]

    def test_m2_version_columns_without_scd2(self) -> None:
        model = dataclasses.replace(
            RETAIL,
            node_types=(dataclasses.replace(CUSTOMER, scd=SCDKind.SCD1), PRODUCT, SALE),
        )
        data = _holds("M2", _retail(model=model))
        assert data["violation_count"] == 1
        assert data["violations"][0]["node_type"] == "Customer"

    def test_m7_holds_and_null_event_time(self) -> None:
        assert _holds("M7", _retail())["checked"] == 1 + 3
        data = _holds(
            "M7",
            _retail("INSERT INTO fact_sale VALUES ('S4', 'C1', 'P1', NULL, 1)"),
        )
        assert data["violation_count"] == 1
        assert data["violations"][0]["instance"] == ["S4"]

    def test_m7_fact_without_event_time(self) -> None:
        mapping = dataclasses.replace(SALE.mapping, event_time=None)  # type: ignore[arg-type]
        model = dataclasses.replace(
            RETAIL,
            node_types=(CUSTOMER, PRODUCT, dataclasses.replace(SALE, mapping=mapping)),
        )
        data = _holds("M7", _retail(model=model))
        assert data["violations"][0]["problem"] == "declares no event_time"

    def test_empty_scope_holds_vacuously_with_nothing_checked(self) -> None:
        unversioned = dataclasses.replace(
            CUSTOMER,
            scd=None,
            mapping=NodeTable("dim_customer", ("customer_id",), {"segment": "segment"}),
        )
        model = dataclasses.replace(RETAIL, node_types=(unversioned, PRODUCT, SALE))
        data = _holds("M7", _retail(model=model))
        assert data == {
            "holds": True,
            "checked": 0,
            "violation_count": 0,
            "violations": [],
        }

    def test_missing_relation_is_a_failure_not_an_empty_scope(self) -> None:
        source = DuckSource(RETAIL, RETAIL_SQL[:2])  # only dim_customer exists
        result = _failed("M7", source, ToolErrorKind.TOOL_FAILURE)
        assert result.error is not None and "exception" in result.error.context
        assert source.teardowns == 1


# ---------------------------------------------------------------------------
# M3, M6 — VERSION_KEY references
# ---------------------------------------------------------------------------

ORDER_SQL = RETAIL_SQL[:2] + [
    # version 101 is claimed by two entities: a broken surrogate.
    "INSERT INTO dim_customer VALUES (101, 'C2', 'retail', '2022-01-01', '2023-01-01')",
    "CREATE TABLE fact_order(order_id VARCHAR, customer_vk INTEGER, ordered_at DATE)",
    "INSERT INTO fact_order VALUES ('O1', 101, '2023-05-01'), ('O2', 201, '2023-05-01')",
]
ORDER = NodeType(
    "Order",
    NodeKind.FACT,
    {},
    NodeTable("fact_order", ("order_id",), event_time="ordered_at"),
)
ORDERED = EdgeType(
    "Customer",
    "Order",
    "ordered",
    EdgeKind.VERB,
    mapping=ForeignKey(Holder.TARGET, ("customer_vk",), Reference.VERSION_KEY),
)
ORDERS = DGMModel((CUSTOMER, ORDER), (ORDERED,))


class TestReferences:
    def test_m3_reference_yielding_two_instances(self) -> None:
        data = _holds("M3", DuckSource(ORDERS, ORDER_SQL))
        assert data["checked"] == 1 + 2
        assert data["violation_count"] == 1
        assert data["violations"][0]["holder_row"] == ["O1"]

    def test_m3_holds_on_entity_key_references(self) -> None:
        data = _holds("M3", _retail())
        assert data["holds"] is True and data["checked"] == 2 + 3 + 3

    def test_m6_surrogate_of_two_entities(self) -> None:
        data = _holds("M6", DuckSource(ORDERS, ORDER_SQL))
        assert data["checked"] == 1 + 3
        assert data["violations"] == [
            {
                "node_type": "Customer",
                "version_key": 101,
                "problem": "the version surrogate belongs to more than one entity",
            }
        ]

    def test_m6_version_key_reference_to_unversioned_type(self) -> None:
        plain_customer = dataclasses.replace(
            CUSTOMER,
            scd=None,
            mapping=NodeTable("dim_customer", ("customer_id",), {"segment": "segment"}),
        )
        model = DGMModel((plain_customer, ORDER), (ORDERED,))
        data = _holds("M6", DuckSource(model, ORDER_SQL))
        assert data["violation_count"] == 1
        assert (
            data["violations"][0]["problem"]
            == "the referenced type declares no version_key"
        )


# ---------------------------------------------------------------------------
# PA1, PA2, PA4
# ---------------------------------------------------------------------------


class TestPipelineArtifact:
    @pytest.mark.parametrize("invariant,checked", [("PA1", 2), ("PA2", 2), ("PA4", 2)])
    def test_holds_on_a_valid_history(self, invariant: str, checked: int) -> None:
        data = _holds(invariant, _pipeline())
        assert data["holds"] is True and data["checked"] == checked

    def test_pa1_transition_outside_delta(self) -> None:
        data = _holds(
            "PA1",
            _pipeline(
                "INSERT INTO transitions VALUES ('T3', 'R1', 'T2', 'Complete', "
                "'scheduled', 'In-flight', '2024-01-02 10:00')"
            ),
        )
        assert data["violation_count"] == 1
        assert data["violations"][0]["transition"] == ["T3"]

    def test_pa2_chain_not_starting_at_missing(self) -> None:
        data = _holds(
            "PA2",
            _pipeline(
                "INSERT INTO artifact VALUES ('R3', 'Complete')",
                "INSERT INTO transitions VALUES ('T5', 'R3', NULL, 'In-flight', "
                "'succeeded', 'Complete', '2024-01-01 10:00')",
            ),
        )
        assert data["checked"] == 3
        [witness] = data["violations"]
        assert witness["artifact"] == ["R3"]
        assert witness["problems"] == ["the first transition does not start at Missing"]

    def test_pa2_two_heads(self) -> None:
        data = _holds(
            "PA2",
            _pipeline(
                "INSERT INTO transitions VALUES ('T9', 'R1', NULL, 'Missing', "
                "'scheduled', 'In-flight', '2024-01-01 12:00')"
            ),
        )
        assert data["violations"][0]["problems"] == [
            "the followed_by chain has 2 heads"
        ]

    def test_pa4_state_disagrees_with_history(self) -> None:
        data = _holds(
            "PA4", _pipeline("UPDATE artifact SET state = 'Stale' WHERE run_id = 'R1'")
        )
        assert data["violations"][0]["state"] == "Stale"
        assert data["violations"][0]["state_at_now"] == "Complete"

    def test_pa4_versioned_artifact_is_not_implemented(self) -> None:
        model = dataclasses.replace(
            PIPELINE,
            node_types=(dataclasses.replace(RUN, scd=SCDKind.SCD2), TRANSITION),
        )
        _failed("PA4", _pipeline(model=model), ToolErrorKind.NOT_IMPLEMENTED)

    def test_missing_pipeline_edge_is_unsupported(self) -> None:
        model = dataclasses.replace(PIPELINE, edge_types=(TRANSITION_OF,))
        _failed("PA2", _pipeline(model=model), ToolErrorKind.UNSUPPORTED)


# ---------------------------------------------------------------------------
# Evaluator-level guarantees
# ---------------------------------------------------------------------------


class TestEvaluator:
    def test_cancellation_is_not_a_result(self) -> None:
        cancel = CancelToken()
        cancel.cancel()
        with pytest.raises(NotEvaluable) as info:
            evaluate_invariant("M1", RETAIL, None, 5, cancel=cancel)
        assert info.value.kind is ToolErrorKind.CANCELLED

    def test_teardown_runs_after_success(self) -> None:
        source = _retail()
        _holds("H3", source)
        assert source.setups == 1 and source.teardowns == 1
