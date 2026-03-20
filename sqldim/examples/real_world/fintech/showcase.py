"""
sqldim Showcase: Fintech Payment Intelligence Pipeline
------------------------------------------------------
Pillar 1: SCD Type 2  — Account risk-tier history
Pillar 2: Dual-Paradigm — Money-movement graph (TransferEdge traversal)
Pillar 3: Array Metric  — Month-partitioned balance history
Pillar 4: Drift Observatory — Schema-evolution observability
"""

import asyncio
from datetime import date
from sqlmodel import Session, SQLModel, create_engine, select
from sqlalchemy.pool import StaticPool

from sqldim.examples.real_world.fintech.models import (
    Account,
    TransferEdge,
    MonthlyBalanceFact,
)
from sqldim.examples.datasets.domains.fintech import AccountsSource
from sqldim.core.kimball.dimensions.scd.handler import SCDHandler
from sqldim.core.graph import GraphModel
from sqldim.observability.drift import DriftObservatory

_ACCOUNT_FACTORY_KEYS = frozenset({"account_id"})


def _strip(row: dict, drop: frozenset) -> dict:
    return {k: v for k, v in row.items() if k not in drop}


def _version_tag(is_current: bool) -> str:
    return "current" if is_current else "expired"


async def _ft_pillar1_scd_accounts(engine) -> None:
    """SCD Type 2 — Account Risk-Tier History (AccountsSource factory)."""
    src = AccountsSource(n_entities=3, seed=42)
    with Session(engine) as session:
        handler = SCDHandler(
            Account, session, track_columns=["risk_tier", "account_type"]
        )
        await handler.process([_strip(r, _ACCOUNT_FACTORY_KEYS) for r in src.initial])
        await handler.process([_strip(r, _ACCOUNT_FACTORY_KEYS) for r in src.events])
        versions = session.exec(
            select(Account).order_by(Account.account_number, Account.valid_from)
        ).all()

    upgraded = sum(not v.is_current for v in versions)
    print("✅ Pillar 1: SCD Type 2 — Account Risk-Tier History")
    print(f"   Loaded {len(src.initial)} accounts via AccountsSource factory.")
    print(
        f"   Total version rows in dim_account: {len(versions)} "
        f"({upgraded} expired / escalated)."
    )
    for v in versions:
        print(
            f"     {v.owner_name:<18}  risk={v.risk_tier:<8} ({_version_tag(v.is_current)})"
        )
    print()


async def _ft_pillar2_payment_graph(engine) -> tuple:
    """Payment Graph — Money-Movement Traversal."""
    with Session(engine) as session:
        # Use the first 3 current accounts from the SCD dimension
        current_accts = session.exec(
            select(Account)
            .where(Account.is_current == True)  # noqa: E712
            .order_by(Account.id)
            .limit(3)
        ).all()
        while len(current_accts) < 3:  # pragma: no cover
            current_accts = list(current_accts)  # ensure we have 3 for the graph
            break
        acct_a, acct_b, acct_c = current_accts[0], current_accts[1], current_accts[2]
        transfers = [
            TransferEdge(
                subject_id=acct_a.id,
                object_id=acct_b.id,
                transfer_date=date(2024, 5, 1),
                amount_usd=12_500.0,
                reference="REF-A",
            ),
            TransferEdge(
                subject_id=acct_b.id,
                object_id=acct_c.id,
                transfer_date=date(2024, 5, 3),
                amount_usd=11_000.0,
                reference="REF-B",
            ),
            TransferEdge(
                subject_id=acct_c.id,
                object_id=acct_a.id,
                transfer_date=date(2024, 5, 5),
                amount_usd=10_500.0,
                reference="REF-C",
            ),
        ]
        session.add_all(transfers)
        session.commit()
        graph = GraphModel(Account, TransferEdge, session=session)
        a_vertex = await graph.get_vertex(Account, acct_a.id)
        first_hop = await graph.neighbors(a_vertex, edge_type=TransferEdge)
        total_outflow = sum(
            t.amount_usd for t in transfers if t.subject_id == acct_a.id
        )
        acct_a_id, acct_b_id, acct_c_id = acct_a.id, acct_b.id, acct_c.id
        a_name, b_name = acct_a.owner_name, acct_b.owner_name

    print("✅ Pillar 2: Payment Graph — Money-Movement Traversal")
    print(f"   Circular chain: {a_name} → {b_name} → … → {a_name}")
    print(f"   {a_name}'s first-hop neighbours: {[v.owner_name for v in first_hop]}")
    print(f"   {a_name}'s total outflow: ${total_outflow:,.2f}")
    print()
    return acct_a_id, acct_b_id, acct_c_id


async def _ft_pillar3_balance_history(engine, emma_id: int, franz_id: int) -> None:
    """Array Metric — Month-Partitioned Balance History."""
    with Session(engine) as session:
        balances = [
            MonthlyBalanceFact(
                account_id=emma_id,
                month_start=date(2024, 5, 1),
                balance_usd=24_320.50,
                balance_history=[24_000.0, 24_200.0, 24_320.5],
            ),
            MonthlyBalanceFact(
                account_id=franz_id,
                month_start=date(2024, 5, 1),
                balance_usd=8_190.00,
                balance_history=[8_400.0, 8_300.0, 8_190.0],
            ),
        ]
        session.add_all(balances)
        session.commit()
        stored = session.exec(select(MonthlyBalanceFact)).all()

    print("✅ Pillar 3: Array Metric — Month-Partitioned Balance History")
    print(f"   {len(stored)} balance rows; each stores a JSON daily-balance array.")
    for b in stored:
        trend = "↑" if b.balance_history[-1] >= b.balance_history[0] else "↓"
        print(
            f"     account_id={b.account_id}  "
            f"month={b.month_start}  "
            f"closing=${b.balance_usd:,.2f}  trend={trend}"
        )
    print()


async def _ft_pillar4_drift_observatory() -> None:
    """Drift Observatory — Schema-Evolution Observability."""
    obs = DriftObservatory.in_memory()
    from sqldim.contracts.engine import EvolutionChange, EvolutionReport

    report = EvolutionReport()
    report.safe_changes = [
        EvolutionChange("added", "geo_region", "nullable VARCHAR added"),
    ]
    obs.ingest_evolution(
        report,
        dataset="fact_txn",
        run_id="run-fintech-001",
        layer="silver",
        pipeline_name="fintech-pipeline",
    )
    rate_df = obs.breaking_change_rate()
    rows = rate_df.fetchall()
    print("✅ Pillar 4: Drift Observatory — Schema-Evolution Tracking")
    print("   Ingested 1 schema evolution event (add_column → fact_txn).")
    print(f"   Breaking change rate query returned {len(rows)} result row(s).")


async def run_fintech_showcase():
    """End-to-end fintech payment intelligence showcase.

    Demonstrates SCD Type 2 account risk history, graph traversal over
    payment transfers, month-partitioned balance arrays, and real-time
    schema-evolution observability via :class:`DriftObservatory`.
    """
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    print("🏦 Fintech Payment Intelligence Pipeline\n")
    await _ft_pillar1_scd_accounts(engine)
    acct_a_id, acct_b_id, _acct_c_id = await _ft_pillar2_payment_graph(engine)
    await _ft_pillar3_balance_history(engine, acct_a_id, acct_b_id)
    await _ft_pillar4_drift_observatory()
    print(
        "\n🚀 Fintech Pipeline complete. "
        "SCD2 + Payment Graph + Array Metrics + Drift Observability."
    )
    engine.dispose()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_fintech_showcase())
