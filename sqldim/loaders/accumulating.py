from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type
from sqlmodel import Session, select
from sqldim.core.models import FactModel


class AccumulatingLoader:
    """
    Loads Accumulating Snapshot Fact Tables — one row per business process instance,
    updated in-place as it moves through pipeline stages.

    Classic example: an order progressing through placed → approved → shipped → delivered.
    Each stage has its own date FK. Rows are updated, not inserted, as stages complete.

    Usage:
        loader = AccumulatingLoader(
            fact=OrderPipelineFact,
            match_column="order_id",
            milestone_columns=["approved_at", "shipped_at", "delivered_at"],
            session=session,
        )
        loader.process(records)
    """

    def __init__(
        self,
        fact: Type[FactModel],
        match_column: str,
        milestone_columns: List[str],
        session: Session,
    ):
        self.fact = fact
        self.match_column = match_column
        self.milestone_columns = milestone_columns
        self.session = session

    def process(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Process incoming records. For each:
        - If no existing row: INSERT
        - If existing row: UPDATE milestone columns that are now non-null

        Returns a dict with counts: {"inserted": n, "updated": n}
        """
        inserted = 0
        updated = 0

        for record in records:
            match_val = record.get(self.match_column)
            existing = self.session.exec(
                select(self.fact).where(
                    getattr(self.fact, self.match_column) == match_val
                )
            ).first()

            if existing is None:
                row = self.fact(**record)
                self.session.add(row)
                inserted += 1
            else:
                # Only update milestone columns that are newly populated
                changed = False
                for col in self.milestone_columns:
                    new_val = record.get(col)
                    if new_val is not None and getattr(existing, col, None) is None:
                        setattr(existing, col, new_val)
                        changed = True
                # Also allow updating any non-milestone, non-match columns
                for col, val in record.items():
                    if col not in self.milestone_columns and col != self.match_column:
                        setattr(existing, col, val)
                        changed = True
                if changed:
                    self.session.add(existing)
                    updated += 1

        self.session.commit()
        return {"inserted": inserted, "updated": updated}
