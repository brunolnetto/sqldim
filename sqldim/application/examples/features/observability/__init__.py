"""Observability star-schema showcase.

Demonstrates the full bronze → silver → gold observability medallion pipeline
where schema drift and quality violations are modelled as first-class Kimball
dimensional facts.

Run directly:
    python -m sqldim.application.examples.features.observability.showcase
"""

from sqldim.application.examples.features.observability.showcase import run_showcase

__all__ = ["run_showcase"]
