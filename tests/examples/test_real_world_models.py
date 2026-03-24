"""Tests for real-world showcase model files — drives 100% line coverage.

Importing and introspecting the SQLModel classes exercises every class-body
line (field declarations are evaluated at import time in Python).
"""

from sqldim.application.examples.real_world.saas_growth.models import (
    User,
    ReferralEdge,
    UserActivity,
)
from sqldim.application.examples.real_world.user_activity.models import (
    Device,
    Event,
    UserCumulated,
)


# ---------------------------------------------------------------------------
# saas_growth/models.py
# ---------------------------------------------------------------------------


class TestSaasGrowthModels:
    """Import-and-introspect to cover every line of saas_growth/models.py."""

    def test_user_natural_key_and_vertex_type(self):
        assert User.__natural_key__ == ["email"]
        assert User.__vertex_type__ == "user"

    def test_user_default_plan_tier(self):
        assert User.model_fields["plan_tier"].default == "free"

    def test_referral_edge_types(self):
        assert ReferralEdge.__edge_type__ == "referred"
        assert ReferralEdge.__directed__ is True

    def test_referral_edge_subject_and_object(self):
        assert ReferralEdge.__subject__ is User
        assert ReferralEdge.__object__ is User

    def test_user_activity_grain(self):
        assert "one row per user per month" in UserActivity.__grain__

    def test_user_activity_has_dates_active_field(self):
        assert "dates_active" in UserActivity.model_fields


# ---------------------------------------------------------------------------
# user_activity/models.py
# ---------------------------------------------------------------------------


class TestUserActivityModels:
    """Import-and-introspect to cover every line of user_activity/models.py."""

    def test_device_natural_key_and_scd_type(self):
        assert Device.__natural_key__ == ["device_id"]
        assert Device.__scd_type__ == 1

    def test_device_fields_present(self):
        for field in ("device_id", "browser_type", "os_type", "device_type"):
            assert field in Device.model_fields

    def test_event_grain(self):
        assert "one row per user event" in Event.__grain__

    def test_event_has_device_fk(self):
        assert "device_id" in Event.model_fields

    def test_user_cumulated_grain(self):
        assert "one row per user per date" in UserCumulated.__grain__

    def test_user_cumulated_has_dates_active(self):
        assert "dates_active" in UserCumulated.model_fields
