"""``dgm_map_analytical_space`` and the §8.16 / §3.4.6 computations (#132).

The tool reads only the model a dataset declares (the ``DGMModel`` that
``dgm_validate_invariant`` reads too), through ``ToolContext.data``; it never
reads instance data, never infers a model and never looks at another dataset.
Every non-success is typed (D42).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from sqldim.core.query.dgm.analytical_space import (
    AGG_FNS,
    VOCABULARIES,
    DGMSchema,
    SchemaDeclarationError,
    admissible_anchor_sets,
    decompose,
    validate_anchor_set,
)
from sqldim.core.query.dgm.annotations import (
    FactlessFact,
    Grain,
    GrainKind,
    MedallionLayer,
    MedallionLayerKind,
    RolePlaying,
)
from sqldim.core.query.dgm.model import (
    AnnotationNotDeclared,
    DGMModel,
    EdgeKind,
    EdgeType,
    ModelNotDeclared,
    NodeKind,
    NodeType,
    PropType,
)
from sqldim.core.query.dgm.nl.supervisor._auth import Action
from sqldim.core.query.dgm.nl.supervisor._dispatch import execute_tool
from sqldim.core.query.dgm.nl.supervisor._schemas import (
    DEFAULT_POLICY,
    MapAnalyticalSpaceOutput,
    SupervisorPolicy,
)
from sqldim.core.query.dgm.nl.supervisor._tools import TOOL_REGISTRY, build_registry
from sqldim.core.query.dgm.nl.supervisor._tools_space import MapAnalyticalSpaceTool
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    ToolCallRequest,
    ToolCallResult,
    ToolErrorKind,
)
from sqldim.core.query.dgm.nl.supervisor._validation import bind_plan
from tests.supervisor._fakes import auth, plan, step, tool_context

TOOL = "dgm_map_analytical_space"


SPACE_KINDS = frozenset({"FactlessFact", "MedallionLayer", "RolePlaying"})
"""The kinds of Σ the decomposition reads, stated completely by the fixtures."""

INTEGER, STRING = PropType("Integer"), PropType("String")
DIM, FACT = NodeKind.DIM, NodeKind.FACT
VERB, BRIDGE = EdgeKind.VERB, EdgeKind.BRIDGE


def retail(*sigma: Any, kinds: frozenset[str] = SPACE_KINDS) -> DGMSchema:
    """Customer → Sale ← Date (two roles), Product → Sale; Region is isolated."""
    return DGMModel(
        node_types=(
            NodeType("Customer", DIM, {"age": INTEGER, "name": STRING}),
            NodeType("Product", DIM, {"category": STRING}),
            NodeType("Date", DIM, {"day": PropType("Date")}),
            NodeType("Sale", FACT, {"revenue": PropType("Real")}),
            NodeType("Region", DIM, {"code": STRING}),
        ),
        edge_types=(
            EdgeType("Customer", "Sale", "placed", VERB),
            EdgeType("Product", "Sale", "included_in", VERB),
            EdgeType("Date", "Sale", "ordered_on", VERB),
            EdgeType("Date", "Sale", "shipped_on", VERB),
        ),
        sigma=sigma,
        sigma_kinds=kinds | {type(a).__name__ for a in sigma},
    )


class Source:
    """A dataset handle that declares a model and has data it must not touch."""

    def __init__(self, schema: Any = None, *, declares: bool = True) -> None:
        self._schema = schema
        self.instance_reads = 0
        if declares:
            self.dgm_model = self._declare

    def _declare(self) -> Any:
        if isinstance(self._schema, Exception):
            raise self._schema
        return self._schema

    # Instance access: the tool must never call these (§8.11 schema determinacy).
    def setup(self) -> None:
        self.instance_reads += 1

    def get_connection(self) -> Any:
        self.instance_reads += 1
        raise AssertionError("instance data read")


class Opener:
    def __init__(self, sources: dict[str, Source]) -> None:
        self.sources = sources
        self.opened: list[str] = []

    def __call__(self, dataset_id: str) -> Source:
        self.opened.append(dataset_id)
        return self.sources[dataset_id]


def _request(arguments: dict[str, Any]) -> ToolCallRequest:
    bound, errors = bind_plan(plan(step("m", TOOL, arguments, fraction=1.0)))
    assert errors == []
    return bound.steps[0]


def _run(
    arguments: dict[str, Any],
    sources: dict[str, Source],
    grants: Any = None,
    registry: Any = None,
) -> tuple[ToolCallResult, Opener]:
    opener = Opener(sources)
    ctx = tool_context(auth(grants), opener)  # type: ignore[arg-type]
    req = _request(arguments)
    result = asyncio.run(execute_tool(req, registry or build_registry(), ctx))
    return result, opener


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_contract_tool_is_available_without_demo(self) -> None:
        schema = TOOL_REGISTRY[TOOL].capability_schema
        assert schema.available is True
        assert "+demo" not in schema.version
        assert schema.reads == ("/dataset",)

    def test_registered_in_demo_mode_too(self) -> None:
        assert isinstance(build_registry(demo=True)[TOOL], MapAnalyticalSpaceTool)

    def test_what_can_i_answer_is_a_contract_tool(self) -> None:
        # Implemented by the question lattice (tests/supervisor/test_question_lattice.py).
        tool = TOOL_REGISTRY["dgm_what_can_i_answer"]
        assert tool.capability_schema.available is True
        assert "+demo" not in tool.capability_schema.version

    def test_cost_estimate_reads_no_data(self) -> None:
        cost = TOOL_REGISTRY[TOOL].cost_estimate({"dataset": "sales"}, tool_context())
        assert (cost.cpu_ops, cost.io_ops) == (0, 0)


# ---------------------------------------------------------------------------
# SUCCEEDED: the decomposition of a known schema
# ---------------------------------------------------------------------------


class TestKnownSchema:
    def test_vocabularies_members_and_sizes(self) -> None:
        result, opener = _run({"dataset": "sales"}, {"sales": Source(retail())})
        assert result.status is CallStatus.SUCCEEDED, result.error
        assert result.is_contract_success
        data = result.data
        MapAnalyticalSpaceOutput.model_validate(data)
        vocab = data["vocabularies"]
        assert set(VOCABULARIES) <= set(vocab)
        assert vocab["V_N"] == ["Customer", "Date", "Product", "Region", "Sale"]
        assert vocab["V_E"] == [
            "(Customer, placed, Sale)",
            "(Date, ordered_on, Sale)",
            "(Date, shipped_on, Sale)",
            "(Product, included_in, Sale)",
        ]
        assert vocab["V_P"] == [
            "Customer.age",
            "Customer.name",
            "Date.day",
            "Product.category",
            "Region.code",
            "Sale.revenue",
        ]
        assert "FILTER.threshold[Sale.revenue]: Real" in vocab["V_Par"]
        assert set(AGG_FNS) <= set(vocab["V_Op"])
        assert "TemporalMode.EVENTUALLY" in vocab["V_Pred"]
        assert data["sizes"] == {k: len(v) for k, v in vocab.items()}
        assert data["grounding"] is None
        assert result.datasets == ["sales"]
        assert opener.opened == ["sales"]

    def test_reads_no_instance_data(self) -> None:
        source = Source(retail())
        result, _ = _run({"dataset": "sales"}, {"sales": source})
        assert result.status is CallStatus.SUCCEEDED
        assert source.instance_reads == 0

    def test_anchor_sets_respect_as1_as2_as3(self) -> None:
        result, _ = _run({"dataset": "sales"}, {"sales": Source(retail())})
        sets = result.data["anchor_sets"]
        assert ["Customer", "Sale"] in sets
        assert ["Customer", "Date", "Product"] in sets
        # AS2: two Date anchors only with distinct declared roles.
        assert ["Date@ordered_on", "Date@shipped_on"] in sets
        assert ["Date", "Date"] not in sets
        # AS1: the isolated Region joins nothing; it is not silently dropped.
        assert not any("Region" in s for s in sets)
        # AS3 and k ≥ 2.
        assert all(2 <= len(s) <= DEFAULT_POLICY.max_anchors for s in sets)
        assert sets == sorted(sets, key=lambda s: (len(s), s))

    def test_sigma_projections_are_applied_and_listed(self) -> None:
        schema = retail(
            FactlessFact("Sale"), MedallionLayer("Region", MedallionLayerKind.BRONZE)
        )
        result, _ = _run({"dataset": "sales"}, {"sales": Source(schema)})
        vocab = result.data["vocabularies"]
        assert "AGGREGATION.agg_fn[Sale]: {COUNT}" in vocab["V_Par"]
        assert any("BRONZE types {Region}" in p for p in vocab["V_Par"])
        assert vocab["Σ"] == [
            "FactlessFact(fact=Sale)",
            "MedallionLayer(node=Region, layer=BRONZE)",
        ]

    def test_schema_without_edges_has_no_anchor_set(self) -> None:
        schema = DGMModel(
            (NodeType("Only", DIM, {"x": INTEGER}),), sigma_kinds=SPACE_KINDS
        )
        result, _ = _run({"dataset": "sales"}, {"sales": Source(schema)})
        assert result.status is CallStatus.SUCCEEDED
        assert result.data["vocabularies"]["V_N"] == ["Only"]
        assert result.data["vocabularies"]["V_E"] == []
        assert result.data["anchor_sets"] == []

    def test_bounds_come_from_policy(self) -> None:
        policy = SupervisorPolicy(max_enum_hops=1, max_anchors=2)
        tool = MapAnalyticalSpaceTool(policy)
        reg = dict(build_registry())
        reg[TOOL] = tool
        result, _ = _run({"dataset": "sales"}, {"sales": Source(retail())}, None, reg)
        sets = result.data["anchor_sets"]
        assert ["Customer", "Sale"] in sets
        assert ["Customer", "Date"] not in sets  # 2 hops > max_enum_hops
        assert all(len(s) == 2 for s in sets)


# ---------------------------------------------------------------------------
# Failures: each typed, none answered by something else
# ---------------------------------------------------------------------------


class TestFailClosed:
    def test_ungranted_dataset_is_unauthorized_and_opens_nothing(self) -> None:
        result, opener = _run(
            {"dataset": "secret"},
            {"secret": Source(retail()), "sales": Source(retail())},
        )
        assert result.status is CallStatus.FAILED
        assert result.error.kind is ToolErrorKind.UNAUTHORIZED
        assert result.error.message == "not authorized or not found"
        assert opener.opened == []

    def test_unknown_dataset_is_reported_like_an_ungranted_one(self) -> None:
        result, opener = _run({"dataset": "nope"}, {})
        assert result.error.kind is ToolErrorKind.UNAUTHORIZED
        assert "sales" not in result.error.message
        assert opener.opened == []

    def test_correlate_only_grant_is_not_read(self) -> None:
        result, opener = _run(
            {"dataset": "sales"},
            {"sales": Source(retail())},
            grants={"sales": {Action.CORRELATE}},
        )
        assert result.error.kind is ToolErrorKind.UNAUTHORIZED
        assert opener.opened == []

    @pytest.mark.parametrize(
        "arguments",
        [{}, {"dataset": ""}, {"dataset": "sales", "extra": 1}, {"dataset": 3}],
    )
    def test_invalid_arguments(self, arguments: dict[str, Any]) -> None:
        opener = Opener({"sales": Source(retail())})
        req = ToolCallRequest(
            call_id="m",
            tool_name=TOOL,
            arguments=arguments,
            budget_fraction=1.0,
            datasets=["sales"],
            idempotency_key="k",
        )
        result = asyncio.run(
            execute_tool(req, build_registry(), tool_context(opener=opener))  # type: ignore[arg-type]
        )
        assert result.error.kind is ToolErrorKind.INVALID_ARGUMENTS
        assert opener.opened == []

    def test_undeclared_schema_is_unsupported_not_empty(self) -> None:
        result, _ = _run({"dataset": "sales"}, {"sales": Source(declares=False)})
        assert result.status is CallStatus.FAILED
        assert result.data is None
        assert result.error.kind is ToolErrorKind.UNSUPPORTED

    def test_model_not_declared_is_unsupported(self) -> None:
        source = Source(ModelNotDeclared("no model"))
        result, _ = _run({"dataset": "sales"}, {"sales": source})
        assert result.error.kind is ToolErrorKind.UNSUPPORTED

    @pytest.mark.parametrize("kind", sorted(SPACE_KINDS))
    def test_undeclared_annotation_kind_is_unsupported(self, kind: str) -> None:
        schema = retail(kinds=SPACE_KINDS - {kind})
        result, _ = _run({"dataset": "sales"}, {"sales": Source(schema)})
        assert result.status is CallStatus.FAILED and result.data is None
        assert result.error.kind is ToolErrorKind.UNSUPPORTED
        assert result.error.context["annotation"] == kind

    def test_declaration_violating_2_1_is_tool_failure(self) -> None:
        schema = DGMModel(
            (NodeType("F", FACT), NodeType("D", DIM)),
            (EdgeType("F", "D", "x", VERB),),
            sigma_kinds=SPACE_KINDS,
        )
        result, _ = _run({"dataset": "sales"}, {"sales": Source(schema)})
        assert result.error.kind is ToolErrorKind.TOOL_FAILURE
        assert "must start at a dimension type" in result.error.context["detail"]

    def test_declaration_error_is_tool_failure(self) -> None:
        bad = SchemaDeclarationError("verb edge type is dim → fact")
        result, _ = _run({"dataset": "sales"}, {"sales": Source(bad)})
        assert result.error.kind is ToolErrorKind.TOOL_FAILURE
        assert "dim → fact" in result.error.context["detail"]

    def test_declaration_that_raises_is_tool_failure(self) -> None:
        result, _ = _run({"dataset": "sales"}, {"sales": Source(OSError("down"))})
        assert result.error.kind is ToolErrorKind.TOOL_FAILURE
        assert result.error.context["exception"] == "OSError"

    @pytest.mark.parametrize("declared", [{"V_N": ["x"]}, None])
    def test_wrong_declaration_type_is_tool_failure(self, declared: Any) -> None:
        result, _ = _run({"dataset": "sales"}, {"sales": Source(declared)})
        assert result.error.kind is ToolErrorKind.TOOL_FAILURE

    def test_utterance_is_not_implemented_and_opens_nothing(self) -> None:
        result, opener = _run(
            {"dataset": "sales", "utterance": "revenue by region"},
            {"sales": Source(retail())},
        )
        assert result.status is CallStatus.FAILED
        assert result.error.kind is ToolErrorKind.NOT_IMPLEMENTED
        assert "§12.8" in result.error.context["missing"]
        assert opener.opened == []

    def test_output_schema_rejects_empty_v_n(self) -> None:
        with pytest.raises(ValueError):
            MapAnalyticalSpaceOutput.model_validate(
                {"vocabularies": {"V_N": []}, "sizes": {}, "anchor_sets": []}
            )


# ---------------------------------------------------------------------------
# Pure computations
# ---------------------------------------------------------------------------


def _dims(*names: str) -> tuple[NodeType, ...]:
    return tuple(NodeType(n, DIM) for n in names)


def _model(*sigma: Any, kinds: Any = None, edges: Any = ()) -> DGMModel:
    kinds = frozenset({type(a).__name__ for a in sigma}) if kinds is None else kinds
    return DGMModel(_dims("D"), edges, sigma, kinds)


class TestSchemaDeclaration:
    """Ill-formed declarations are rejected at construction (§2.1, §2.4)."""

    @pytest.mark.parametrize(
        "build,needle",
        [
            (lambda: DGMModel(()), "N_S is empty"),
            (
                lambda: DGMModel((NodeType("A", DIM), NodeType("A", FACT))),
                "declared twice",
            ),
            (lambda: NodeType("A", "cube"), "τ_N"),  # type: ignore[arg-type]
            (lambda: NodeType("A@x", DIM), "invalid node type"),
            (lambda: NodeType("A", DIM, {"": INTEGER}), "non-empty"),
            (lambda: NodeType("A", DIM, {"k": "Int"}), "not a PropType"),  # type: ignore[dict-item]
            (lambda: PropType("Decimal"), "unknown property type"),
            (
                lambda: _model(edges=(EdgeType("D", "Z", "x", BRIDGE),)),
                "endpoint",
            ),
            (
                lambda: _model(
                    edges=(
                        EdgeType("D", "D", "x", BRIDGE),
                        EdgeType("D", "D", "x", BRIDGE),
                    )
                ),
                "declared twice",
            ),
            (lambda: _model("Grain"), "Σ"),
            (
                lambda: _model(FactlessFact("D"), kinds=frozenset()),
                "not in sigma_kinds",
            ),
            (lambda: _model(FactlessFact("D")), "not a fact type"),
            (lambda: _model(Grain("Ghost", GrainKind.EVENT)), "'Ghost'"),
            (lambda: _model(kinds=frozenset({"Folklore"})), "unknown annotation kinds"),
            (
                lambda: _model(
                    MedallionLayer("D", MedallionLayerKind.GOLD),
                    MedallionLayer("D", MedallionLayerKind.BRONZE),
                ),
                "declared twice",
            ),
        ],
    )
    def test_ill_formed_declaration(self, build: Any, needle: str) -> None:
        with pytest.raises(SchemaDeclarationError, match=needle):
            build()

    def test_invariants_are_not_construction_checks(self) -> None:
        # A verb edge from a fact breaks PARTITION (§2.1): the model is
        # well-formed, dgm_validate_invariant reports it, this tool refuses it.
        model = DGMModel(
            (NodeType("F", FACT), NodeType("D", DIM)),
            (EdgeType("F", "D", "x", VERB),),
        )
        assert model.partition_problems(model.edge_types[0]) == [
            "a verb edge type must start at a dimension type",
            "a verb edge type must end at a fact type",
        ]

    def test_undeclared_kind_is_never_read_as_none(self) -> None:
        with pytest.raises(AnnotationNotDeclared) as info:
            decompose(retail(kinds=frozenset({"MedallionLayer", "RolePlaying"})))
        assert info.value.kind == "FactlessFact"

    def test_kinds_are_accepted_as_strings(self) -> None:
        node = NodeType("A", "fact")  # type: ignore[arg-type]
        assert node.kind is NodeKind.FACT


class TestAnchorSets:
    def _check(self, anchors: list[str], **kw: Any) -> list[str]:
        bounds = {"max_hops": 4, "max_anchors": 3, **kw}
        return validate_anchor_set(retail(), anchors, **bounds)

    def test_valid(self) -> None:
        assert self._check(["Customer", "Product"]) == []

    def test_unknown_type(self) -> None:
        assert "not a node type" in self._check(["Customer", "Ghost"])[0]

    def test_single_anchor(self) -> None:
        assert any("at least two" in p for p in self._check(["Customer"]))

    def test_as1_disconnected(self) -> None:
        assert any(p.startswith("AS1") for p in self._check(["Customer", "Region"]))

    def test_as1_hop_bound(self) -> None:
        assert any(
            p.startswith("AS1") for p in self._check(["Customer", "Date"], max_hops=1)
        )

    def test_as2_same_type_needs_distinct_roles(self) -> None:
        assert any(p.startswith("AS2") for p in self._check(["Date", "Date"]))
        assert any(p.startswith("AS2") for p in self._check(["Date@a", "Date@b"]))
        assert self._check(["Date@ordered_on", "Date@shipped_on"]) == []

    def test_as2_roles_from_role_playing(self) -> None:
        schema = DGMModel(
            _dims("Emp"),
            (EdgeType("Emp", "Emp", "reports_to", BRIDGE),),
            sigma=(RolePlaying("Emp", ["manager", "report"]),),
            sigma_kinds=SPACE_KINDS,
        )
        assert (
            validate_anchor_set(
                schema, ["Emp@manager", "Emp@report"], max_hops=1, max_anchors=2
            )
            == []
        )
        sets = admissible_anchor_sets(schema, max_hops=1, max_anchors=2)
        assert ["Emp@manager", "Emp@report"] in sets

    def test_as3_bound(self) -> None:
        problems = self._check(["Customer", "Product", "Date", "Sale"])
        assert any(p.startswith("AS3") for p in problems)

    def test_decompose_is_schema_determined(self) -> None:
        assert decompose(retail()) == decompose(retail())
