# ADR 0011 — Cache entries over unversioned sources are opt-in

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #141, #136
- **Amends:** ADR 0004
- **Spec sections:** §8.14, §10.4 (D34, D41)

## Context

ADR 0004 keys cached results by the watermarks of every source the plan can read. A source without a watermark cannot reveal that it changed. For such sources ADR 0004 adopted a fallback: serve the entry until `CACHE_TTL_UNVERSIONED` expires, flagged `possibly_stale = true`. That serves a possibly outdated answer to a caller that never agreed to receive one. Audit #141 does not allow a fallback of this kind unless the caller selected it.

## Options

1. Keep the TTL fallback of ADR 0004 as the default.
2. **Opt-in staleness.** By default, never serve an entry that depends on an unversioned source. Serve it only when the caller states the age it accepts.
3. Never cache a plan that depends on an unversioned source.

## Decision

Option 2, chosen by the maintainer.

```
cache[result_key] = (result_set, computed_at, unversioned)
CachePolicy(accept_stale: Duration | None = None)      -- supplied by the caller with each request

lookup(result_key, policy):
  e = cache.get(result_key)
  if e = NONE:                                          miss
  if not e.unversioned:                                 hit                      -- ADR 0004 rule
  if policy.accept_stale ≠ None
     and now − e.computed_at ≤ policy.accept_stale:     hit, possibly_stale = true, computed_at reported
  otherwise:                                            miss — the plan runs
```

Entries over unversioned sources may still be stored, so that a later caller who accepts staleness can use them. No component sets `accept_stale` on the caller's behalf. `CACHE_TTL_UNVERSIONED` is removed.

## Rationale

- By default every hit is provably equal to re-execution, as in ADR 0004's correctness argument; the exception exists only by request.
- Callers that value latency over freshness, such as dashboards over append-only logs, keep a bounded and flagged stale read.
- Option 3 forbids even an explicit request for staleness. Option 1 is the fallback the audit rejects.

## Consequences

- The cache API takes a `CachePolicy`. Results report `possibly_stale` and `computed_at` whenever a stale entry was served.
- Tests distinguish: versioned hit; unversioned miss by default; unversioned hit within `accept_stale`; unversioned miss beyond it.
