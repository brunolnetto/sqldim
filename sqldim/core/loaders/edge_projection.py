"""
LazyEdgeProjectionLoader — graph edge projection via DuckDB SQL.
Reproduces player_game_edges.sql and player_player_edges.sql.
"""

from __future__ import annotations

import asyncio
from sqldim.core.loaders._utils import _resolve_table



# ---------------------------------------------------------------------------
# Lazy (DuckDB-first) loader — no Python data, no OOM risk
# ---------------------------------------------------------------------------


class LazyEdgeProjectionLoader:
    """
    Edge projection loader using DuckDB SQL.

    Two projection modes:

    * **Direct** — reads (subject_key, object_key) pairs directly from
      the source fact table.
    * **Self-join** — joins the fact table with itself on *self_join_key*
      to generate entity↔entity co-occurrence edges (e.g.
      player–player edges from shared game appearances).

    Additional columns can be mapped from source to edge via *property_map*
    (``{source_col: edge_col}``).

    Usage::

        with DuckDBSink("/tmp/dev.duckdb") as sink:
            # Direct edge projection
            loader = LazyEdgeProjectionLoader(
                table_name="graph_player_game",
                subject_key="player_id",
                object_key="game_id",
                sink=sink,
            )
            rows = loader.process("player_game_facts.parquet")

            # Self-join (co-player edges)
            loader = LazyEdgeProjectionLoader(
                table_name="graph_player_player",
                subject_key="player_id",
                object_key="player_id",
                self_join=True,
                self_join_key="game_id",
                property_map={"pts": "shared_points"},
                sink=sink,
            )
            rows = loader.process("player_game_facts.parquet")
    """

    def __init__(
        self,
        table: str | type,
        subject_key: str,
        object_key: str,
        sink,
        property_map: Optional[Dict[str, str]] = None,
        self_join: bool = False,
        self_join_key: Optional[str] = None,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.table_name = _resolve_table(table)
        self.subject_key = subject_key
        self.object_key = object_key
        self.sink = sink
        self.property_map = property_map or {}
        self.self_join = self_join
        self.self_join_key = self_join_key
        self.batch_size = batch_size
        self._con = con or _duckdb.connect()

    def process(self, source) -> int:
        """
        Project *source* into edge rows and write to *table_name*.
        Returns rows written.
        """
        from sqldim.sources import coerce_source

        _sql = coerce_source(source).as_sql(self._con)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS
            SELECT * FROM ({_sql})
        """)

        if self.self_join:
            self._build_self_join_view()
        else:
            self._build_direct_view()

        return self.sink.write(self._con, "edge_view", self.table_name, self.batch_size)

    async def aload(self, source) -> int:
        """Async wrapper — runs :meth:`process` in a thread pool executor."""
        return await asyncio.to_thread(self.process, source)

    #: Alias for :meth:`process` — unified sync entry point across all loaders.
    load = process

    def _build_direct_view(self) -> None:
        sk = self.subject_key
        ok = self.object_key
        prop_cols = (
            ", "
            + ", ".join(f"{src} AS {tgt}" for src, tgt in self.property_map.items())
            if self.property_map
            else ""
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW edge_view AS
            SELECT
                {sk} AS subject_id,
                {ok} AS object_id{prop_cols}
            FROM incoming
        """)

    def _build_self_join_view(self) -> None:
        sk = self.subject_key
        sjk = self.self_join_key
        prop_cols = (
            ", "
            + ", ".join(
                f"f1.{src} + f2.{src} AS {tgt}"
                for src, tgt in self.property_map.items()
            )
            if self.property_map
            else ""
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW edge_view AS
            SELECT
                f1.{sk} AS subject_id,
                f2.{sk} AS object_id{prop_cols}
            FROM incoming f1
            JOIN incoming f2
              ON f1.{sjk} = f2.{sjk}
             AND f1.{sk} < f2.{sk}
        """)
