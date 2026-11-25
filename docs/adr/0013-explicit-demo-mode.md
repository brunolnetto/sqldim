# ADR 0013 — The heuristic path survives only as an explicit demo mode

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #141; affects #129, #143, #149
- **Spec sections:** §12.2 (fail closed), §10.4 (D47)

## Context

The re-evaluation of the code against the frozen specification (`fc4e2dd`) found that `sqldim ask`, `chat`, `showtime`, `supervisor` and the eval suites work today only through a heuristic path, which is not the DGM pipeline:

- a keyword planner;
- template SQL and an LLM SQL agent, contrary to D26;
- supervisor tools that return placeholders;
- NL nodes replaced by passthrough stubs.

Applying the fail-closed rules of ADR 0012 removes all of it. Until the real pipeline exists (#143, #149, #150, #148, #129–#134), every one of those commands would fail with `NOT_IMPLEMENTED`.

## Options

1. **Explicit demo mode.** Keep the heuristic path behind an explicit per-run selection. Its results are marked as demo output and never count as contract successes.
2. Fail closed immediately. The commands and eval suites become unavailable until each capability is real.
3. Keep the current behaviour until the real implementation is complete, then switch. This violates the audit rule during the transition.

## Decision

Option 1, chosen by the maintainer.

- The heuristic path runs only when the caller selects it for that run, for example with a `--demo` flag or `demo=True` in the API. Nothing selects it automatically, including when a model, dataset or component is missing.
- Every result it produces carries `demo = true`. It is never reported as a `SUCCEEDED` contract result, never stored in the knowledge base as a finding, and never used as evidence of conformance in tests or in documentation.
- Without the selection, the components it stands in for are unavailable (`available = false`) and fail closed (ADR 0012).
- Tests of the heuristic path are labelled as demo tests. Conformance tests never run in demo mode.

## Rationale

- Keeps demonstrations and examples usable without letting their output pass for DGM results.
- The selection is explicit and visible in every result, which is the only kind of alternative behaviour audit #141 allows.
- Option 2 removes useful demonstrations for months. Option 3 keeps the silent fallbacks the audit exists to remove.

## Consequences

- #129 and #143 move the heuristic code behind the demo selection instead of deleting it outright, and add the `demo` marker to results.
- CLI help and documentation (#140) describe demo mode as a demonstration, not as a capability.
- When a real capability lands, its demo counterpart can be deleted; demo mode is expected to shrink to nothing.
