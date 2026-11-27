"""DGM Supervisor — reading the model a dataset declares (§2.6, §12.3).

``dgm_validate_invariant`` and ``dgm_map_analytical_space`` read one
declaration, :class:`~sqldim.core.query.dgm.model.DGMModel`, from the handle
``ctx.data.open(dataset)`` returns.  :func:`declared_model` maps each way that
can fail to one typed outcome, so both tools report them alike (D42):

* the handle has no ``dgm_model()``, or it raises ``ModelNotDeclared`` →
  ``UNSUPPORTED``;
* the declaration is ill-formed (``ModelDeclarationError``), raises anything
  else, or returns something that is not a ``DGMModel`` → ``TOOL_FAILURE``.

A tool that then reads an annotation kind the model does not state gets
``AnnotationNotDeclared``; :func:`undeclared_annotation` is its typed outcome
(``UNSUPPORTED``).
"""

from __future__ import annotations

from typing import Any

from sqldim.core.query.dgm.model import (
    AnnotationNotDeclared,
    DGMModel,
    ModelDeclarationError,
    ModelNotDeclared,
)
from sqldim.core.query.dgm.nl.supervisor._types import ToolErrorKind

__all__ = ["DeclarationUnavailable", "declared_model", "undeclared_annotation"]

NO_MODEL = (
    "the dataset declares no DGM model (G_S, K_S, M, Σ; §2.6); it is never "
    "inferred from tables"
)


class DeclarationUnavailable(Exception):
    """The dataset's model cannot be read; ``kind`` is the typed outcome."""

    def __init__(self, kind: ToolErrorKind, message: str, **context: Any) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.context = context


def declared_model(source: Any, dataset: str) -> DGMModel:
    """The model *source* declares, or :class:`DeclarationUnavailable`."""
    declare = getattr(source, "dgm_model", None)
    if not callable(declare):
        raise DeclarationUnavailable(
            ToolErrorKind.UNSUPPORTED, NO_MODEL, dataset=dataset
        )
    try:
        model = declare()
    except ModelNotDeclared as exc:
        raise DeclarationUnavailable(
            ToolErrorKind.UNSUPPORTED, NO_MODEL, dataset=dataset, detail=str(exc)
        ) from exc
    except ModelDeclarationError as exc:
        raise DeclarationUnavailable(
            ToolErrorKind.TOOL_FAILURE,
            "the dataset's model declaration is ill-formed",
            dataset=dataset,
            detail=str(exc),
        ) from exc
    except Exception as exc:  # noqa: BLE001 — reported, never swallowed
        raise DeclarationUnavailable(
            ToolErrorKind.TOOL_FAILURE,
            "the dataset's model declaration failed",
            dataset=dataset,
            exception=type(exc).__name__,
            detail=str(exc),
        ) from exc
    if not isinstance(model, DGMModel):
        raise DeclarationUnavailable(
            ToolErrorKind.TOOL_FAILURE,
            "the dataset's dgm_model() did not return a DGMModel",
            dataset=dataset,
            declared=type(model).__name__,
        )
    return model


def undeclared_annotation(exc: AnnotationNotDeclared) -> DeclarationUnavailable:
    """The typed outcome of a read of an annotation kind the model does not state."""
    return DeclarationUnavailable(
        ToolErrorKind.UNSUPPORTED, str(exc), annotation=exc.kind
    )
