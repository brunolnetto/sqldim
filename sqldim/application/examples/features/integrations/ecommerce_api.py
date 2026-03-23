"""
application/examples/features/integrations/ecommerce_api.py
============================================================
Minimal Flask REST API that fronts an in-memory DuckDB store of ecommerce
customer data.  Intended to be used as the live HTTP target for the dlt
pipeline in ``flask_dlt.py``.

Endpoints
---------
GET  /customers                     — full customer list (JSON array)
POST /customers/<id>/upgrade        — tier-upgrade event; body: {"tier": "gold"}
"""

from __future__ import annotations

from typing import Any

from flask import Flask, jsonify, request

_app = Flask(__name__)
_store: dict[int, dict[str, Any]] = {}  # customer_id -> row dict


def build_flask_app(customers: list[dict[str, Any]]) -> Flask:
    """Populate the in-memory store and return the configured Flask app."""
    _store.clear()
    for row in customers:
        _store[row["customer_id"]] = dict(row)
    return _app


@_app.route("/customers", methods=["GET"])
def get_customers():
    """Return all customers as a JSON array, optionally filtered by tier."""
    tier = request.args.get("tier")
    rows = list(_store.values())
    if tier:
        rows = [r for r in rows if r.get("tier") == tier]
    return jsonify(rows)


@_app.route("/customers/<int:customer_id>/upgrade", methods=["POST"])
def upgrade_customer(customer_id: int):
    """
    Upgrade a customer's tier.

    Body (JSON): {"tier": "<new_tier>"}  (defaults to "gold")
    Returns the updated customer row, or 404 if not found.
    """
    if customer_id not in _store:
        return jsonify({"error": f"customer {customer_id} not found"}), 404
    body = request.get_json(silent=True) or {}
    new_tier = body.get("loyalty_tier", "gold")
    _store[customer_id]["loyalty_tier"] = new_tier
    return jsonify(_store[customer_id])


def get_store() -> dict[int, dict[str, Any]]:
    """Return the current in-memory store (useful for debugging)."""
    return _store
