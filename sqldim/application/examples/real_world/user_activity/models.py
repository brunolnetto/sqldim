"""Kimball models for the user-activity real-world example pipeline.

Defines three models used by ``showcase.py``: :class:`Device` (SCD-1),
:class:`Event` (transaction fact), and :class:`UserCumulated` (cumulative fact
with a bitmask datelist column for L7/L28 retention metrics).
"""

from __future__ import annotations
import datetime
from typing import List, Optional
from sqlmodel import Field, Column, JSON

from sqldim import DimensionModel, FactModel
from sqldim.core.kimball.mixins import DatelistMixin


class Device(DimensionModel, table=True):
    """Device dimension — captures browser, OS, and device-type attributes.

    SCD Type 1: attributes are overwritten in place; no version history is
    kept because device properties change infrequently and history is not
    needed for retention metrics.
    """

    __natural_key__ = ["device_id"]
    __scd_type__ = 1

    id: Optional[int] = Field(default=None, primary_key=True)
    device_id: int
    browser_type: str
    os_type: str
    device_type: str


class Event(FactModel, table=True):
    """Raw event stream fact table — one row per user interaction.

    Reproduces ``events.sql``; each device_id FK resolves to a
    :class:`Device` dimension row via the surrogate key.
    """

    __grain__ = "one row per user event"

    id: Optional[int] = Field(default=None, primary_key=True)
    url: str
    user_id: int
    device_id: int = Field(foreign_key="device.id")
    event_time: datetime.datetime


class UserCumulated(FactModel, DatelistMixin, table=True):
    """Cumulative user activity fact — one row per user per calendar date.

    ``dates_active`` stores a JSON-serialised list of :class:`datetime.date`
    objects representing all days the user was active.  Reproduces the
    ``users_cumulated.sql`` model from the data-engineering exercises.
    """

    __grain__ = "one row per user per date"

    user_id: int = Field(primary_key=True)
    date: datetime.date = Field(primary_key=True)
    # Stored as JSON for SQLite compat, would be DATE[] in Postgres
    dates_active: List[datetime.date] = Field(
        default_factory=list, sa_column=Column(JSON)
    )
