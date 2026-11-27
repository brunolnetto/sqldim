"""Schema-grounded registry lookups of §11.3 / §11.10.

* :class:`SchemaVocabulary` — ``schema.property_keys(T)``, the domain of
  ``K_S(T)`` of a declared node type.
* :class:`VerbLabels` — ``verb_labels[(from, to)]``, the labels of the edge
  types declared from one node type to another.
* :class:`GroundedEntityRegistry` — the §11.3 ``EntityRegistry`` with
  ``alias_types`` bound to a declared schema.

Each lookup answers for one type or one pair, never with the whole
vocabulary, and an unknown node type is :class:`UnknownIdentifier`, not an
empty answer: "no edge between two known types" and "no such type" are
different results.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType

from sqldim.core.query.dgm.analytical_space import DGMSchema
from sqldim.core.query.dgm.nl._types import EntityRegistry
from sqldim.core.query.dgm.planner._ir import check_identifier
from sqldim.core.query.dgm.runtime._errors import (
    InvalidRuntimeArgument,
    UnknownIdentifier,
)

__all__ = ["SchemaVocabulary", "VerbLabels", "GroundedEntityRegistry"]


class SchemaVocabulary:
    """The ``schema`` of ``DGMRuntime``: lookups over a declared ``G_S``."""

    def __init__(self, schema: DGMSchema) -> None:
        if not isinstance(schema, DGMSchema):
            raise InvalidRuntimeArgument("SchemaVocabulary needs a DGMSchema")
        self.schema = schema
        self._props = {n.name: tuple(k for k, _ in n.properties) for n in schema.nodes}

    @property
    def node_types(self) -> frozenset[str]:
        return frozenset(self._props)

    def require_type(self, node_type: str) -> str:
        if node_type not in self._props:
            raise UnknownIdentifier(f"node type {node_type!r} is not declared")
        return node_type

    def property_keys(self, node_type: str) -> list[str]:
        """``dom K_S(node_type)`` in declaration order."""
        return list(self._props[self.require_type(node_type)])


class VerbLabels(Mapping[tuple[str, str], tuple[str, ...]]):
    """``verb_labels[(from_type, to_type)]`` — labels of declared edge types.

    Keys are directed as declared (``source → target``).  A pair of known
    types with no edge type maps to ``()``; a pair naming an unknown type
    raises :class:`UnknownIdentifier` from ``[]`` *and* from :meth:`get`,
    which therefore never substitutes a default for a failed lookup.
    Iteration yields only the pairs that have at least one label.
    """

    def __init__(self, vocab: SchemaVocabulary) -> None:
        self._vocab = vocab
        labels: dict[tuple[str, str], list[str]] = {}
        for e in vocab.schema.edges:
            labels.setdefault((e.source, e.target), []).append(e.label)
        self._labels = {k: tuple(sorted(v)) for k, v in labels.items()}

    def __getitem__(self, pair: tuple[str, str]) -> tuple[str, ...]:
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise InvalidRuntimeArgument("verb_labels is keyed by (from_type, to_type)")
        src, dst = pair
        self._vocab.require_type(src)
        self._vocab.require_type(dst)
        return self._labels.get((src, dst), ())

    def get(self, pair: tuple[str, str], default: object = None) -> tuple[str, ...]:  # type: ignore[override]
        """As ``[]``; *default* is never returned for an unknown type."""
        return self[pair]

    def __contains__(self, pair: object) -> bool:
        return pair in self._labels

    def __iter__(self) -> Iterator[tuple[str, str]]:
        return iter(sorted(self._labels))

    def __len__(self) -> int:
        return len(self._labels)


class GroundedEntityRegistry(EntityRegistry):
    """An :class:`EntityRegistry` whose aliases are bound to declared types.

    ``alias_types`` maps each alias to a node type of the schema and is fixed
    at construction; ``node_terms`` may only name a bound alias.
    """

    def __init__(self, vocab: SchemaVocabulary, aliases: Mapping[str, str]) -> None:
        super().__init__()
        bound: dict[str, str] = {}
        for alias, node_type in aliases.items():
            check_identifier(alias, "alias")
            bound[alias] = vocab.require_type(node_type)
        self.vocab = vocab
        self.alias_types: Mapping[str, str] = MappingProxyType(bound)
        self.verb_labels = VerbLabels(vocab)

    def register_node(self, nl_term: str, alias: str) -> None:
        if alias not in self.alias_types:
            raise UnknownIdentifier(f"alias {alias!r} is not bound to a node type")
        super().register_node(nl_term, alias)

    def prop_refs(self, node_type: str) -> list[str]:
        """``alias.prop`` for every alias of *node_type* (``get_valid_prop_refs``)."""
        keys = self.vocab.property_keys(node_type)
        return [
            f"{alias}.{prop}"
            for alias, t in sorted(self.alias_types.items())
            if t == node_type
            for prop in keys
        ]
