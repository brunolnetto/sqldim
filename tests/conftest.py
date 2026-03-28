"""Session-scoped fixtures shared across the entire test suite.

Pre-import heavy AI SDK modules once at session start.

The pydantic-ai model factories in ``sqldim.core.query.dgm.nl._agents``
use lazy imports (``from pydantic_ai.models.X import XModel`` inside the
factory body) to avoid loading AI SDKs at package import time.  The
downside is that the first test in the session to exercise each factory
pays the full cold-import cost:

  * ``pydantic_ai.models.openai``  +  ``openai`` SDK     ≈  25–30 s
  * ``pydantic_ai.models.anthropic`` + ``anthropic`` SDK  ≈  30–35 s
  * ``pydantic_ai.models.google``  + ``google.genai``     ≈  90–100 s
  * ``pydantic_ai.models.groq``    + ``groq`` SDK         ≈  8–12 s

``tests/cli/test_pipeline_sources.py::TestMakeModel`` is the first
collector to exercise these factories (``tests/cli/`` sorts before
``tests/query/``), so those tests appear as extreme outliers.

The ``_preimport_ai_sdks`` fixture below is session-scoped and runs
automatically before any test is collected or executed.  The import cost
is paid once during pytest session setup and is not attributed to any
individual test, removing the outliers entirely.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session", autouse=True)
def _preimport_ai_sdks() -> None:
    """Pre-import heavy pydantic-ai model modules once at session start.

    Each ``try/except ImportError`` guard ensures the fixture is safe even
    when optional AI extras are not installed in the test environment.
    """
    # openai / Ollama-compatible provider
    try:
        from pydantic_ai.models.openai import OpenAIChatModel  # noqa: F401
        from pydantic_ai.providers.openai import OpenAIProvider  # noqa: F401
    except ImportError:
        pass

    # Anthropic
    try:
        from pydantic_ai.models.anthropic import AnthropicModel  # noqa: F401
    except ImportError:
        pass

    # Google Gemini (google.genai — the heaviest import by far)
    try:
        from pydantic_ai.models.google import GoogleModel  # noqa: F401
    except ImportError:
        pass

    # Groq
    try:
        from pydantic_ai.models.groq import GroqModel  # noqa: F401
    except ImportError:
        pass

    # Mistral (optional extra — skip silently when not installed)
    try:
        from pydantic_ai.models.mistral import MistralModel  # noqa: F401
    except ImportError:
        pass
