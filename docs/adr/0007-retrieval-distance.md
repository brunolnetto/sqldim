# ADR 0007 — Retrieval distance d_L is the shortest path of local refinement moves

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #84
- **Spec sections:** §8.16.6, §12.7, §10.4 (D37)

## Context

`dgm_retrieve_related` returns stored findings with `d_L(q, q') ≤ k`. The specification never defined `d_L`: it alternated between "adjacency in the BDD", refinement relations and position in parameter space. Without a definition, retrieval and its ranking are not reproducible.

## Options

1. **Shortest path in the graph of refinement moves**, with unit cost or declared weights; parameters handled separately.
2. **Distance weighted by changes in the BDD** of the predicates.
3. **A composite distance on `(q, π)`**, a weighted sum of a structural and a parametric distance.

## Decision

Option 1, chosen by the maintainer, with parameters as a separate tie-breaker.

### Definition

The **move graph** `M` has the canonical skeletons of `Q_valid` (§8.11) as vertices. There is an edge between two skeletons when one structural local move of §8.16.6 turns one into the other:

- add or remove one atom in Where;
- replace one GroupBy key;
- replace one hop of one path pattern;
- replace the AggFn of one aggregate.

Every such move has an inverse move of the same kind, so `M` is undirected. Then:

```
d_S(q, q')  = length of a shortest path from q to q' in M      -- ∞ if none
d_P(π, π')  = number of parameter slots on which π and π' differ (for q = q'), else undefined
d_L((q, π), (q', π')) = d_S(q, q')                              -- the retrieval radius applies to this
```

`d_S` is a metric on each connected component of `M`: unit-weight shortest-path distance in an undirected graph is non-negative, symmetric, zero only on equal vertices, and satisfies the triangle inequality.

### Retrieval

```
retrieve_related((q', π'), k):
  candidates = findings f with d_S(f.q, q') ≤ k, computed by breadth-first search from q' to depth k
  keep only findings whose datasets are all granted to the principal (§12.10)
  order by  d_S ascending,  then d_P(f.π, π') ascending when f.q = q',  then stored_at descending,
            then finding id ascending                         -- a total, reproducible order
```

The search visits at most `Σ_{j≤k} N^j` skeletons, where `N` is the neighbourhood bound of §8.16.6 restricted to structural moves. `k` is a parameter with default 2.

### Example

`q' = revenue by region, Where segment = 'retail'`. A finding on `revenue by region` with no Where is at `d_S = 1` (remove one atom). A finding on `revenue by country, Where segment = 'retail'` is at `d_S = 1` (replace one GroupBy key). A finding on `order count by country` is at `d_S = 3`: remove the atom, replace the key, replace the aggregate.

## Rationale

- Uses moves the specification already defines and bounds (§8.16.6); no new structure is introduced.
- Reproducible: the distance and the tie-breaking order are total and do not depend on data.
- Option 2 would tie retrieval to the BDD's variable order and to the encoding details of §6.1. Option 3 needs weights with no principled values yet.

## Consequences

- §12.7 states `d_L` and the retrieval order; "lattice adjacency in the BDD" is replaced by this definition.
- Findings whose skeletons lie in different connected components of `M`, or in different anchor sets, are at infinite distance and are never retrieved by distance.
- Weighted moves are a possible extension: declared weights per move kind turn `d_S` into a weighted shortest path without changing the rest of the rule.
