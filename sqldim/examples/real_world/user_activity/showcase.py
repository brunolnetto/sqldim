"""
sqldim Showcase: User Activity & Retention (Bitmask Pattern)
-----------------------------------------------------------
Demonstrates the power of the DatelistMixin and LazyBitmaskLoader.
Replaces 32 boolean columns with a single integer for L7/L28 metrics.
"""

import asyncio
from datetime import date, timedelta
from sqlmodel import Session, SQLModel, create_engine
from sqlalchemy.pool import StaticPool
import duckdb
from sqldim.examples.real_world.user_activity.models import Device, UserCumulated
from sqldim.core.loaders.bitmask import LazyBitmaskLoader


class _InProcessSink:
    """Minimal DuckDB-backed sink for showcase use."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con = con

    def write(self, con, view_name: str, table_name: str, batch_size: int) -> int:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {view_name}")
        return con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


async def run_activity_showcase():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    session = Session(engine)

    print("🖱️ Initializing User Activity Showcase: Bitmask Retention\n")

    # 1. Create a Device and User
    iphone = Device(
        device_id=1, browser_type="Safari", os_type="iOS", device_type="Mobile"
    )
    session.add(iphone)
    session.commit()

    # 2. Synthesize 30 days of activity for a "Loyal User"
    # User was active every day for the first week, then missed some days.
    ref_date = date(2024, 3, 1)
    active_dates = [
        ref_date - timedelta(days=i) for i in [0, 1, 2, 3, 4, 5, 6, 10, 15, 20]
    ]

    loyal_user = UserCumulated(
        user_id=101,
        date=ref_date,
        # Convert dates to strings for JSON serialization in SQLite
        dates_active=[d.isoformat() for d in active_dates],
    )
    session.add(loyal_user)
    session.commit()
    print("✅ Pillar 1: User activity accumulated for User 101.")
    print(f"   Dates active: {len(active_dates)} unique days stored in a list.\n")

    # 3. BITMASK ENCODING: The Efficiency Pillar
    # Use LazyBitmaskLoader — all computation stays inside DuckDB SQL,
    # no Python element-wise loop over the date list.
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE activity_input AS
        SELECT 101 AS user_id, ?::VARCHAR[] AS dates_active
    """, [loyal_user.dates_active])

    with _InProcessSink(con) as sink:
        loader = LazyBitmaskLoader(
            table="fact_activity_bitmask",
            partition_key="user_id",
            dates_column="dates_active",
            reference_date=ref_date,
            window_days=32,
            sink=sink,
            con=con,
        )
        loader.process("activity_input")

    bitmask = con.execute(
        "SELECT datelist_int FROM fact_activity_bitmask WHERE user_id = 101"
    ).fetchone()[0]
    con.close()

    print("✅ Pillar 2: Activity encoded into 32-bit integer.")
    print(f"   Integer Value: {bitmask}")
    print(f"   Binary Representation: {bin(bitmask)}\n")

    # 4. ANALYTICS: L7 / L28 Metrics
    # We use the DatelistMixin methods on the model instance
    print("✅ Pillar 3: Retention Metrics (via bitwise ops)")
    print(f"   Is Active Today?        : {loyal_user.activity_in_window(1, ref_date)}")
    print(f"   L7 (Active days last 7) : {loyal_user.l7(ref_date)}")
    print(f"   L28 (Active days last 28): {loyal_user.l28(ref_date)}")
    print(
        f"   Active in last 30 days? : {loyal_user.activity_in_window(30, ref_date)}\n"
    )

    print("🚀 Showcase complete. From raw lists to high-performance bitmask analytics.")
    session.close()
    engine.dispose()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_activity_showcase())
