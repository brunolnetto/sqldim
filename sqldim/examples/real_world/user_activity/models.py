from __future__ import annotations
import datetime
from typing import List, Optional
from sqlmodel import Field, Column, JSON

from sqldim import DimensionModel, FactModel
from sqldim.core.mixins import DatelistMixin


class Device(DimensionModel, table=True):
    """Device dimension. Reproduces devices.sql."""
    __natural_key__ = ["device_id"]
    __scd_type__ = 1

    id: Optional[int] = Field(default=None, primary_key=True)
    device_id: int
    browser_type: str
    os_type: str
    device_type: str


class Event(FactModel, table=True):
    """Raw event stream. Reproduces events.sql."""
    __grain__ = "one row per user event"

    id: Optional[int] = Field(default=None, primary_key=True)
    url: str
    user_id: int
    device_id: int = Field(foreign_key="device.id")
    event_time: datetime.datetime


class UserCumulated(FactModel, DatelistMixin, table=True):
    """Cumulative user activity. Reproduces users_cumulated.sql."""
    __grain__ = "one row per user per date"

    user_id: int = Field(primary_key=True)
    date: datetime.date = Field(primary_key=True)
    # Stored as JSON for SQLite compat, would be DATE[] in Postgres
    dates_active: List[datetime.date] = Field(
        default_factory=list,
        sa_column=Column(JSON)
    )
