"""Typed failures of the §11.10 runtime APIs.

Every runtime operation either works under its contract or raises one of
these.  Each carries a stable ``code`` so a caller — the NL graph of §11.10,
the supervisor — can report the failure without parsing a message:

==================  ==========================================================
``UNAUTHORIZED``    no :class:`AuthContext`, or a grant is missing (§12.10)
``UNKNOWN``         an identifier the runtime does not hold
``INVALID``         an argument that violates the operation's contract
``UNSUPPORTED``     the operation is defined, but not for this input
``NOT_IMPLEMENTED`` the capability is specified but not built yet; the
                    message names the issue that builds it
``NOT_CONFIGURED``  a dependency the host must supply is missing (no default)
``NOT_EXECUTABLE``  a decision or mode that does not execute a plan
``SUBMISSION``      an ASYNC submission that was not accepted
``NO_CANDIDATES``   a candidate request with nothing to offer
==================  ==========================================================
"""

from __future__ import annotations

from sqldim.core.query.dgm.nl.supervisor._auth import NOT_AUTHORIZED_OR_NOT_FOUND
from sqldim.exceptions import SemanticError

__all__ = [
    "RuntimeAPIError",
    "Unauthorized",
    "UnknownIdentifier",
    "InvalidRuntimeArgument",
    "UnsupportedOperation",
    "NotImplementedCapability",
    "NotConfigured",
    "NotExecutable",
    "SubmissionFailed",
    "NoCandidates",
]


class RuntimeAPIError(SemanticError):
    """Base class; ``code`` names the failure kind."""

    code: str = "RUNTIME_ERROR"


class Unauthorized(RuntimeAPIError, PermissionError):
    """A call without an AuthContext, or without the grants it needs (D44).

    The message never says whether a dataset exists (§12.10 non-disclosure).
    """

    code = "UNAUTHORIZED"

    def __init__(self) -> None:
        super().__init__(NOT_AUTHORIZED_OR_NOT_FOUND)


class UnknownIdentifier(RuntimeAPIError, LookupError):
    code = "UNKNOWN"


class InvalidRuntimeArgument(RuntimeAPIError, ValueError):
    code = "INVALID"


class UnsupportedOperation(RuntimeAPIError):
    code = "UNSUPPORTED"


class NotImplementedCapability(RuntimeAPIError, NotImplementedError):
    code = "NOT_IMPLEMENTED"


class NotConfigured(RuntimeAPIError):
    code = "NOT_CONFIGURED"


class NotExecutable(RuntimeAPIError):
    code = "NOT_EXECUTABLE"


class SubmissionFailed(RuntimeAPIError):
    """An ASYNC submission that produced no accepted job (§11.9)."""

    code = "SUBMISSION"


class NoCandidates(RuntimeAPIError):
    code = "NO_CANDIDATES"
