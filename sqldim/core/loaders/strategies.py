"""Low-level bulk-insert and upsert strategies for SQLModel sessions.

:class:`BulkInsertStrategy` uses ``session.add_all()`` for fast initial loads.
:class:`UpsertStrategy` uses SQLite’s ``INSERT … ON CONFLICT DO UPDATE``
for idempotent re-runs.  Both implement the same :meth:`execute` interface.
"""

from typing import Any, Type
from sqlmodel import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


class BulkInsertStrategy:
    """
    Fast bulk insert using a single session.add_all().
    Best for initial loads with no existing data.
    """

    def execute(
        self, session: Session, model: Type, records: list[dict[str, Any]]
    ) -> int:
        rows = [model(**r) for r in records]
        session.add_all(rows)
        session.commit()
        return len(rows)


class UpsertStrategy:
    """
    INSERT ... ON CONFLICT DO UPDATE (SQLite dialect).
    Safe for re-runs — existing rows are updated, new rows inserted.
    """

    def __init__(self, conflict_column: str):
        self.conflict_column = conflict_column

    def _upsert_stmt(self, table, record: dict[str, Any]):
        stmt = sqlite_insert(table).values(**record)
        update_dict = {k: v for k, v in record.items() if k != self.conflict_column}
        if update_dict:
            return stmt.on_conflict_do_update(
                index_elements=[self.conflict_column],
                set_=update_dict,
            )
        return stmt.on_conflict_do_nothing()

    def execute(
        self, session: Session, model: Type, records: list[dict[str, Any]]
    ) -> int:
        if not records:
            return 0
        table = model.__table__
        for record in records:
            session.execute(self._upsert_stmt(table, record))
        session.commit()
        return len(records)


class MergeStrategy:
    """
    Full MERGE semantics: match on natural key, update if changed, insert if new.
    Database-agnostic implementation using SELECT + conditional INSERT/UPDATE.
    """

    def __init__(self, match_column: str):
        self.match_column = match_column

    def execute(
        self, session: Session, model: Type, records: list[dict[str, Any]]
    ) -> int:
        from sqlmodel import select

        upserted = 0
        for record in records:
            match_val = record.get(self.match_column)
            existing = session.exec(
                select(model).where(getattr(model, self.match_column) == match_val)
            ).first()
            if existing:
                for k, v in record.items():
                    setattr(existing, k, v)
                session.add(existing)
            else:
                session.add(model(**record))
            upserted += 1
        session.commit()
        return upserted
