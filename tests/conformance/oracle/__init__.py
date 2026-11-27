"""Reference semantics of the DGM core, derived from the specification.

Each module implements definitions of
``docs/motivation/dimensional_graph_modelling.md`` and ``docs/adr/`` directly,
over tiny in-memory values.  Nothing here imports ``sqldim``: the oracle must
not share code, or mistakes, with the implementation under test.  Where the
specification leaves a case open, the module says so and raises
:class:`Undefined` instead of choosing a reading.
"""


class Undefined(ValueError):
    """The specification does not decide this input; the oracle refuses it."""


REJECTED = "REJECTED"
"""Result of a construct the specification rejects at construction."""
