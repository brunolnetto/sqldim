from datetime import date
from typing import Any, Dict, List, Type
from sqlmodel import Session
from sqldim.core.models import DimensionModel, FactModel

class SnapshotLoader:
    """
    Loads a Periodic Snapshot Fact Table — one row per entity per time period.

    Usage:
        loader = SnapshotLoader(
            fact=AccountBalanceFact,
            dimension=AccountDimension,
            snapshot_date=date.today(),
            session=session,
        )
        loader.load(records)
    """

    def __init__(
        self,
        fact: Type[FactModel],
        dimension: Type[DimensionModel],
        snapshot_date: date,
        session: Session,
        date_field: str = "snapshot_date",
    ):
        self.fact = fact
        self.dimension = dimension
        self.snapshot_date = snapshot_date
        self.session = session
        self.date_field = date_field

    def load(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert snapshot rows for the given date.
        Each record is a dict of fact column values. The snapshot_date is
        automatically injected into the date_field column.

        Returns the number of rows inserted.
        """
        inserted = 0
        for record in records:
            row_data = {**record, self.date_field: self.snapshot_date}
            row = self.fact(**row_data)
            self.session.add(row)
            inserted += 1
        self.session.commit()
        return inserted
