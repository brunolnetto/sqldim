"""User domain events for the saas_growth domain."""

from __future__ import annotations
from typing import Any
from sqldim.application.datasets.events import AggregateState, DomainEvent


def _changed_rows(orig_list: list, updated_list: list) -> list:
    return [u for orig, u in zip(orig_list, updated_list) if orig != u]


def _set_user_plan(u: dict, user_id: int, new_tier: str) -> dict:
    if u.get("user_id") == user_id:
        return {**u, "plan_tier": new_tier}
    return u


def _get_user_original_tier(users: list, user_id: int) -> str:
    return next(
        (u.get("plan_tier", "unknown") for u in users if u.get("user_id") == user_id),
        "unknown",
    )


def _log_enterprise_referral_bonus(
    state: AggregateState,
    user_id: int,
    new_tier: str,
    changed: list[dict],
    event_ts: str,
    result: dict[str, list[dict]],
) -> None:
    """Append a referral bonus row when an enterprise upgrade came from a referral."""
    if (
        new_tier != "enterprise"
        or "referral_log" not in state.table_names
        or not changed
    ):
        return
    if changed[0].get("acq_source") != "referral":
        return
    log = state.get("referral_log")
    bonus_row: dict[str, Any] = {
        "user_id": user_id,
        "event": "enterprise_upgrade_bonus",
        "credit": 50,
        "event_ts": event_ts,
    }
    log.append(bonus_row)
    state.update("referral_log", log)
    result["referral_log"] = [bonus_row]


def _log_churn(
    state: AggregateState,
    user_id: int,
    original_tier: str,
    reason: str,
    event_ts: str,
    result: dict[str, list[dict]],
    changed: list[dict],
) -> None:
    """Append a churn record to churn_log if that table is present."""
    if "churn_log" not in state.table_names or not changed:
        return
    log = state.get("churn_log")
    churn_row: dict[str, Any] = {
        "user_id": user_id,
        "plan_tier_before": original_tier,
        "reason": reason,
        "event_ts": event_ts,
    }
    log.append(churn_row)
    state.update("churn_log", log)
    result["churn_log"] = [churn_row]


class UserPlanUpgradedEvent(DomainEvent):
    """
    User plan-tier upgrade — dispatched when a user moves to a higher tier.

    Business rules
    --------------
    - Updates ``plan_tier`` on the matching user row.
    - Enterprise upgrade with ``acq_source == "referral"``: if a
      ``referral_log`` table is present in the aggregate, appends a new
      row recording the bonus credit so referral-attribution pipelines
      can pick it up without re-scanning the full user dimension.

    Parameters (via ``emit``)
    -------------------------
    user_id  : int
        The user upgrading.
    new_tier : str
        Target tier (``"pro"`` or ``"enterprise"``).
    event_ts : str, optional
        ISO timestamp for the upgrade event. Defaults to ``"2024-06-01 09:00:00"``.
    """

    name = "user_plan_upgraded"

    def apply(  # type: ignore[override]
        self,
        state: AggregateState,
        *,
        user_id: int,
        new_tier: str,
        event_ts: str = "2024-06-01 09:00:00",
    ) -> dict[str, list[dict]]:
        users = state.get("saas_users")
        updated_users = [_set_user_plan(u, user_id, new_tier) for u in users]
        changed = _changed_rows(users, updated_users)
        if changed:
            state.update("saas_users", updated_users)
        result: dict[str, list[dict]] = {"saas_users": changed}
        _log_enterprise_referral_bonus(
            state, user_id, new_tier, changed, event_ts, result
        )
        return result


class UserChurnedEvent(DomainEvent):
    """
    User cancels subscription — sets ``plan_tier = "churned"``.

    Business rules
    --------------
    - Updates ``plan_tier`` to ``"churned"`` on the user row.
    - If a ``churn_log`` table is present in the aggregate, appends a
      lightweight churn record so downstream churn-rate metrics can be
      computed cheaply without re-scanning the full user dimension.

    Parameters (via ``emit``)
    -------------------------
    user_id  : int
        The user who churned.
    reason   : str, optional
        Churn reason tag (``"price"``, ``"competitor"``, ``"feature_gap"``,
        etc.).  Defaults to ``"unspecified"``.
    event_ts : str, optional
        ISO timestamp.  Defaults to ``"2024-06-01 09:00:00"``.
    """

    name = "user_churned"

    def apply(  # type: ignore[override]
        self,
        state: AggregateState,
        *,
        user_id: int,
        reason: str = "unspecified",
        event_ts: str = "2024-06-01 09:00:00",
    ) -> dict[str, list[dict]]:
        users = state.get("saas_users")
        original_tier = _get_user_original_tier(users, user_id)
        updated_users = [_set_user_plan(u, user_id, "churned") for u in users]
        changed = _changed_rows(users, updated_users)
        if changed:
            state.update("saas_users", updated_users)
        result: dict[str, list[dict]] = {"saas_users": changed}
        _log_churn(state, user_id, original_tier, reason, event_ts, result, changed)
        return result
