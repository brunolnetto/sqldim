"""User domain events for the saas_growth domain."""
from __future__ import annotations
from typing import Any
from sqldim.application.datasets.events import AggregateState, DomainEvent


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

    def apply(
        self,
        state: AggregateState,
        *,
        user_id: int,
        new_tier: str,
        event_ts: str = "2024-06-01 09:00:00",
    ) -> dict[str, list[dict]]:
        users = state.get("saas_users")
        updated_users = [
            {**u, "plan_tier": new_tier}
            if u.get("user_id") == user_id
            else u
            for u in users
        ]
        changed = [u for orig, u in zip(users, updated_users) if orig != u]
        if changed:
            state.update("saas_users", updated_users)

        result: dict[str, list[dict]] = {"saas_users": changed}

        # Business rule: enterprise upgrade from referral → mark referral bonus
        if (
            new_tier == "enterprise"
            and "referral_log" in state.table_names
            and changed
        ):
            user_row = changed[0]
            if user_row.get("acq_source") == "referral":
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

    def apply(
        self,
        state: AggregateState,
        *,
        user_id: int,
        reason: str = "unspecified",
        event_ts: str = "2024-06-01 09:00:00",
    ) -> dict[str, list[dict]]:
        users = state.get("saas_users")
        original_tier = next(
            (u.get("plan_tier", "unknown") for u in users if u.get("user_id") == user_id),
            "unknown",
        )
        updated_users = [
            {**u, "plan_tier": "churned"}
            if u.get("user_id") == user_id
            else u
            for u in users
        ]
        changed = [u for orig, u in zip(users, updated_users) if orig != u]
        if changed:
            state.update("saas_users", updated_users)

        result: dict[str, list[dict]] = {"saas_users": changed}

        # Optional: append to a churn_log table if present
        if "churn_log" in state.table_names and changed:
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

        return result
