"""
Unit tests for utility functions (_utils.py).
"""

import pytest
from datetime import timedelta

from postgres_cache._utils import parse_duration


class TestParseDurationValid:
    """Valid duration parsing cases."""

    def test_minutes(self):
        assert parse_duration("10m") == timedelta(minutes=10)

    def test_hours(self):
        assert parse_duration("2h") == timedelta(hours=2)

    def test_seconds(self):
        assert parse_duration("30s") == timedelta(seconds=30)

    def test_mixed_hours_minutes(self):
        assert parse_duration("2h30m") == timedelta(hours=2, minutes=30)

    def test_all_units(self):
        assert parse_duration("1d2h30m15s") == timedelta(days=1, hours=2, minutes=30, seconds=15)

    def test_repeated_units(self):
        assert parse_duration("1m2m") == timedelta(minutes=3)


class TestParseDurationInvalid:
    """Invalid duration parsing cases — all must raise ValueError."""

    def test_empty_string(self):
        with pytest.raises(ValueError, match="Empty duration string"):
            parse_duration("")

    def test_missing_unit(self):
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("10")

    def test_invalid_unit(self):
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("10x")

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("10m10")
