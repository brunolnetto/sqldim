"""
sqldim Showcase: User Activity & Retention (Bitmask Pattern)
-----------------------------------------------------------
Demonstrates the power of the DatelistMixin and BitmaskerLoader.
Replaces 32 boolean columns with a single integer for L7/L28 metrics.
"""
import asyncio
from datetime import date, timedelta
from sqlmodel import Session, SQLModel, create_engine, select
from examples.user_activity.models import Device, Event, UserCumulated
from sqldim.loaders.bitmask import BitmaskerLoader
import narwhals as nw

async def run_activity_showcase():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    session = Session(engine)

    print("🖱️ Initializing User Activity Showcase: Bitmask Retention\n")

    # 1. Create a Device and User
    iphone = Device(device_id=1, browser_type="Safari", os_type="iOS", device_type="Mobile")
    session.add(iphone)
    session.commit()

    # 2. Synthesize 30 days of activity for a "Loyal User"
    # User was active every day for the first week, then missed some days.
    ref_date = date(2024, 3, 1)
    active_dates = [ref_date - timedelta(days=i) for i in [0, 1, 2, 3, 4, 5, 6, 10, 15, 20]]
    
    loyal_user = UserCumulated(
        user_id=101,
        date=ref_date,
        # Convert dates to strings for JSON serialization in SQLite
        dates_active=[d.isoformat() for d in active_dates]
    )
    session.add(loyal_user)
    session.commit()
    print(f"✅ Pillar 1: User activity accumulated for User 101.")
    print(f"   Dates active: {len(active_dates)} unique days stored in a list.\n")

    # 3. BITMASK ENCODING: The Efficiency Pillar
    # We transform the list of dates into a single 32-bit integer
    loader = BitmaskerLoader(
        source_model=UserCumulated,
        target_model=None, # In-process check
        session=session,
        reference_date=ref_date
    )
    
    # Simulate the vectorized transformation
    import pandas as pd
    df = nw.from_native(pd.DataFrame([{"dates_active": loyal_user.dates_active}]))
    res = loader.process(df)
    bitmask = nw.to_native(res)["datelist_int"][0]
    
    print(f"✅ Pillar 2: Activity encoded into 32-bit integer.")
    print(f"   Integer Value: {bitmask}")
    print(f"   Binary Representation: {bin(bitmask)}\n")

    # 4. ANALYTICS: L7 / L28 Metrics
    # We use the DatelistMixin methods on the model instance
    print(f"✅ Pillar 3: Retention Metrics (via bitwise ops)")
    print(f"   Is Active Today?        : {loyal_user.activity_in_window(1, ref_date)}")
    print(f"   L7 (Active days last 7) : {loyal_user.l7(ref_date)}")
    print(f"   L28 (Active days last 28): {loyal_user.l28(ref_date)}")
    print(f"   Active in last 30 days? : {loyal_user.activity_in_window(30, ref_date)}\n")

    print("🚀 Showcase complete. From raw lists to high-performance bitmask analytics.")

if __name__ == "__main__":
    asyncio.run(run_activity_showcase())
