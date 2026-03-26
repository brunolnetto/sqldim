"""DGM showcase — Example 17: three-band queries, BDD, planner, pipeline artifact,
QuestionAlgebra, CSE, and CORRELATE recommendations.

Run:
    PYTHONPATH=. python -m sqldim.application.examples.features.dgm.showcase
"""

from __future__ import annotations

import duckdb

from sqldim.application.datasets.domains.dgm import DGMShowcaseSource
from sqldim.application.examples.features.dgm._demos_dql import (
    demo_edge_kind_classification,
    demo_b1_filter,
    demo_b1_path_pred,
    demo_b1_not,
    demo_b1_b2_having,
    demo_b1_b3_qualify,
    demo_full_pipeline,
    demo_bridge_path,
)
from sqldim.application.examples.features.dgm._demos_model import (
    demo_bdd_predicate,
    demo_annotation_sigma,
    demo_planner,
    demo_pipeline_artifact,
)
from sqldim.application.examples.features.dgm._demos_algebra import (
    demo_question_algebra,
    demo_cse,
    demo_correlate,
)

EXAMPLE_METADATA = {
    "name": "dgm",
    "title": "Dimensional Graph Model",
    "description": (
        "Example 17: DGM three-band queries, BDD predicates, planner, and exporters. "
        "Example 6: PipelineArtifact backfill-incremental state machine (D20/D21). "
        "Example 7: §8.13 QuestionAlgebra CTE composition. "
        "Example 8: §6.2 Rule 11 Cross-CTE CSE. "
        "Example 9: §7.2 CORRELATE cross-question recommendations."
    ),
    "entry_point": "run_all",
}


def run_all() -> None:
    con = duckdb.connect()
    DGMShowcaseSource().setup(con)

    demo_edge_kind_classification()
    demo_b1_filter(con)
    demo_b1_path_pred(con)
    demo_b1_not(con)
    demo_b1_b2_having(con)
    demo_b1_b3_qualify(con)
    demo_full_pipeline(con)
    demo_bridge_path(con)
    demo_bdd_predicate()
    demo_annotation_sigma()
    demo_planner()
    demo_pipeline_artifact()
    demo_question_algebra()
    demo_cse()
    demo_correlate()

    con.close()
    print("\nDGM showcase complete.")


if __name__ == "__main__":  # pragma: no cover
    run_all()
