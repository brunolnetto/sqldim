# ADR 0006 — The instance graph is a labelled transition system; Kripke structures are derived

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #66 (depends on #21, #30)
- **Spec sections:** §1, §2.1, §8.1, §10.4 (D36)

## Context

The specification identified the instance graph directly with a Kripke structure `K = (S, R, L)`, with `L = P`. But edges carry labels, and P maps nodes to typed property bags, not to sets of atomic propositions. CTL is defined over Kripke structures whose labels are propositions, while `SignaturePred` and path patterns constrain edge labels — that is, actions. State operators and action operators were mixed without a stated construction.

## Options

1. **Labelled transition system (LTS) as the primary model**, with a Kripke structure derived for each temporal predicate.
2. **Pure Kripke**, expanding edge labels and properties into propositions on states.
3. **A modal logic over actions** (e.g. an action-based CTL), specified directly.

## Decision

Option 1, chosen by the maintainer.

### The LTS

```
T = (S, Act, →, ℓ)

S    = N_I                                         -- node instances
Act  = L × {fwd, rev}                              -- edge label and direction of traversal
→    = {(a, (l, fwd), b) | (a, b, l) ∈ E_I}  ∪  {(b, (l, rev), a) | (a, b, l) ∈ E_I}
ℓ(s) = P(s)                                        -- the property bag of the node
```

Reverse traversal (`VerbHop⁻¹`, a bridge traversed against its direction) is the action `(l, rev)`. Edge properties are attached to the transition `(a, (l, d), b)` they come from.

### The derived Kripke structure of a path predicate

A `PathPred` with pattern `π` and sub-filter `φ` is evaluated on a Kripke structure built from `T` and `π`. Let `A_π` be the pattern's automaton: one position per hop, with a loop for each `Repeat`.

```
K_π = (S_π, R_π, L_π)

S_π  = S × positions(A_π)
R_π  = {((n, i), (m, j)) | n —α→ m in T  and  i —α→ j in A_π}
AP_q = {p⁺, p⁻ | p an atom of φ}                   -- the two-valued encoding of §6.1
L_π((n, i)) = {p⁺ | p is TRUE at n under the aliases bound at position i}
            ∪ {p⁻ | p is FALSE at n under the aliases bound at position i}
```

CTL formulas of the predicate (§4.1) are interpreted on `K_π` over maximal paths (D31). A path of `K_π` from `(start, 0)` projects to a path instance of `π` in `T`, and every path instance arises this way. The quantifiers `A` and `E` therefore range over path instances of the pattern, as §4.1 states.

`SignaturePred` constrains the action word of a run of `T` — the sequence of `(label, direction)` — and is evaluated on runs of `T`, not on states of a `K_π`.

## Rationale

- Edge labels are first-class in the model (patterns, signatures, BridgeSemantics). An LTS keeps them as actions, where they belong, instead of encoding them into states.
- CTL keeps its standard semantics over a genuine Kripke structure whose propositions are exactly the atoms the query uses, including the three-valued encoding.
- The product with the pattern automaton is the standard construction behind evaluating regular path constraints together with temporal operators, and it is what the SQL templates compute.

## Consequences

- §8.1 states the LTS and the derived `K_π` in place of the identification of `G_I` with a Kripke structure; §1 and §2.1 are aligned.
- The transposed graph is the LTS with directions swapped, `fwd ↔ rev`.
- Option 3, an action-based logic, is not needed for the current operators. It would be the extension point if operators over actions, rather than over states, were added.
