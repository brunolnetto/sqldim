"""RelationshipSubgraph endpoint cases and the cone intersection (§3.4.2, §8.7).

The cones are defined by reachability since #139.  ``graph_cone_cycle`` is the
graph on which the earlier simple-path reading broke the identity of §8.7; the
specification states its node sets.  ``graph_cycle`` has cones that differ.
"""

from __future__ import annotations

from tests.conformance.cases import Case

CONES = "§3.4.2 RelationshipSubgraph by reachability"
IDENTITY = "§8.7 Cone intersection theorem (G_A* ∩ G_*B = G_AB)"
ALL4 = ["A", "B", "X", "n"]
_CYCLE_EDGES = [("A", "X", "e"), ("B", "n", "e"), ("X", "B", "e"), ("n", "X", "e")]


def _sub(nodes: list[str], edges: list[tuple[str, str, str]]) -> dict:
    return {"nodes": sorted(nodes), "edges": sorted(edges)}


CASES = [
    Case("CONE-01", "cone", CONES, "N_A*, E_A*: every node and edge is reachable from A",
         "graph_cone_cycle", {"case": "forward", "source": "A"},
         _sub(ALL4, _CYCLE_EDGES)),
    Case("CONE-02", "cone", CONES, "N_*B, E_*B: every node reaches B",
         "graph_cone_cycle", {"case": "backward", "target": "B"},
         _sub(ALL4, _CYCLE_EDGES)),
    Case("CONE-03", "cone", IDENTITY,
         "n ∈ N_AB although no simple path A →* B passes through n",
         "graph_cone_cycle", {"case": "between", "source": "A", "target": "B"},
         _sub(ALL4, _CYCLE_EDGES)),
    Case("CONE-04", "cone", IDENTITY, "N_A* ∩ N_*B and E_A* ∩ E_*B equal G_AB",
         "graph_cone_cycle", {"case": "intersection", "source": "A", "target": "B"},
         _sub(ALL4, _CYCLE_EDGES)),
    Case("CONE-05", "cone", CONES, "forward cone of x1 on a cycle with a tail",
         "graph_cycle", {"case": "forward", "source": "x1"}),
    Case("CONE-06", "cone", CONES, "backward cone of x3 excludes the tail x4",
         "graph_cycle", {"case": "backward", "target": "x3"}),
    Case("CONE-07", "cone", IDENTITY, "G_AB from the definition",
         "graph_cycle", {"case": "between", "source": "x1", "target": "x3"}),
    Case("CONE-08", "cone", IDENTITY, "the intersection of the two cones",
         "graph_cycle", {"case": "intersection", "source": "x1", "target": "x3"}),
    Case("CONE-09", "cone", IDENTITY, "B not reachable from A: G_AB is empty",
         "graph_cycle", {"case": "between", "source": "x4", "target": "x1"},
         _sub([], [])),
]  # fmt: skip
