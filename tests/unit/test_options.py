"""
Unit tests for PostgresCacheOptions and EntryOptions (_options.py).

These tests verify validation logic without any database connection.
"""

import pytest
from datetime import datetime, timedelta, timezone

from cache_postgres._options import (
    PostgresCacheOptions,
    EntryOptions,
    _MIN_SCAN_INTERVAL,
    _DEFAULT_SCAN_INTERVAL,
    _DEFAULT_SLIDING_EXPIRATION,
)


# ===========================================================================
# PostgresCacheOptions
# ===========================================================================


class TestPostgresCacheOptionsValid:
    """Valid construction cases."""

    def test_with_dsn(self):
        opts = PostgresCacheOptions(dsn="postgresql://localhost/db")
        assert opts.dsn == "postgresql://localhost/db"
        assert opts.connection_factory is None

    def test_with_connection_factory(self):
        factory = lambda: object()
        opts = PostgresCacheOptions(connection_factory=factory)
        assert opts.connection_factory is factory
        assert opts.dsn is None

    def test_defaults_schema_table(self):
        opts = PostgresCacheOptions(dsn="postgresql://localhost/db")
        assert opts.schema == "public"
        assert opts.table == "cache"

    def test_custom_schema_table(self):
        opts = PostgresCacheOptions(
            dsn="postgresql://localhost/db",
            schema="myschema",
            table="mycache",
        )
        assert opts.schema == "myschema"
        assert opts.table == "mycache"

    def test_default_create_if_not_exists_false(self):
        opts = PostgresCacheOptions(dsn="postgresql://localhost/db")
        assert opts.create_if_not_exists is False

    def test_default_use_wal_false(self):
        opts = PostgresCacheOptions(dsn="postgresql://localhost/db")
        assert opts.use_wal is False

    def test_default_scan_interval(self):
        opts = PostgresCacheOptions(dsn="postgresql://localhost/db")
        assert opts.expiration_scan_interval == _DEFAULT_SCAN_INTERVAL

    def test_custom_scan_interval_at_minimum(self):
        opts = PostgresCacheOptions(
            dsn="postgresql://localhost/db",
            expiration_scan_interval=_MIN_SCAN_INTERVAL,
        )
        assert opts.expiration_scan_interval == _MIN_SCAN_INTERVAL

    def test_custom_scan_interval_above_minimum(self):
        opts = PostgresCacheOptions(
            dsn="postgresql://localhost/db",
            expiration_scan_interval=timedelta(hours=1),
        )
        assert opts.expiration_scan_interval == timedelta(hours=1)

    def test_default_sliding_expiration(self):
        opts = PostgresCacheOptions(dsn="postgresql://localhost/db")
        assert opts.default_sliding_expiration == _DEFAULT_SLIDING_EXPIRATION

    def test_enable_expiration_scan_default_true(self):
        opts = PostgresCacheOptions(dsn="postgresql://localhost/db")
        assert opts.enable_expiration_scan is True


class TestPostgresCacheOptionsInvalid:
    """Invalid construction cases — all must raise ValueError."""

    def test_no_connection_raises(self):
        """One of dsn or connection_factory is required."""
        with pytest.raises(ValueError, match="dsn.*connection_factory"):
            PostgresCacheOptions()

    def test_both_dsn_and_factory_raises(self):
        """Providing both is ambiguous."""
        with pytest.raises(ValueError, match="exactly one"):
            PostgresCacheOptions(
                dsn="postgresql://localhost/db",
                connection_factory=lambda: object(),
            )

    def test_empty_schema_raises(self):
        with pytest.raises(ValueError, match="schema"):
            PostgresCacheOptions(dsn="postgresql://localhost/db", schema="")

    def test_whitespace_schema_raises(self):
        with pytest.raises(ValueError, match="schema"):
            PostgresCacheOptions(dsn="postgresql://localhost/db", schema="  ")

    def test_empty_table_raises(self):
        with pytest.raises(ValueError, match="table"):
            PostgresCacheOptions(dsn="postgresql://localhost/db", table="")

    def test_scan_interval_below_minimum_raises(self):
        """Scan interval must be >= 5 minutes."""
        with pytest.raises(ValueError, match="expiration_scan_interval"):
            PostgresCacheOptions(
                dsn="postgresql://localhost/db",
                expiration_scan_interval=timedelta(minutes=4, seconds=59),
            )

    def test_scan_interval_zero_raises(self):
        with pytest.raises(ValueError, match="expiration_scan_interval"):
            PostgresCacheOptions(
                dsn="postgresql://localhost/db",
                expiration_scan_interval=timedelta(0),
            )

    def test_default_sliding_zero_raises(self):
        with pytest.raises(ValueError, match="positive"):
            PostgresCacheOptions(
                dsn="postgresql://localhost/db",
                default_sliding_expiration=timedelta(0),
            )

    def test_default_sliding_negative_raises(self):
        with pytest.raises(ValueError, match="positive"):
            PostgresCacheOptions(
                dsn="postgresql://localhost/db",
                default_sliding_expiration=timedelta(seconds=-1),
            )


# ===========================================================================
# EntryOptions
# ===========================================================================


class TestEntryOptionsValid:
    """Valid EntryOptions construction cases."""

    def test_all_none_is_valid(self):
        """No expiration configured — relies on default."""
        opts = EntryOptions()
        assert opts.sliding_expiration is None
        assert opts.absolute_expiration is None
        assert opts.absolute_expiration_relative is None

    def test_only_sliding(self):
        opts = EntryOptions(sliding_expiration=timedelta(minutes=20))
        assert opts.sliding_expiration == timedelta(minutes=20)

    def test_only_absolute_tz_aware(self):
        exp = datetime(2030, 1, 1, tzinfo=timezone.utc)
        opts = EntryOptions(absolute_expiration=exp)
        assert opts.absolute_expiration == exp

    def test_only_relative(self):
        opts = EntryOptions(absolute_expiration_relative=timedelta(hours=1))
        assert opts.absolute_expiration_relative == timedelta(hours=1)

    def test_sliding_and_absolute(self):
        """Sliding + absolute is allowed — they're not mutually exclusive."""
        opts = EntryOptions(
            sliding_expiration=timedelta(minutes=10),
            absolute_expiration=datetime(2030, 1, 1, tzinfo=timezone.utc),
        )
        assert opts.sliding_expiration is not None
        assert opts.absolute_expiration is not None


class TestEntryOptionsInvalid:
    """Invalid EntryOptions — must raise ValueError."""

    def test_both_absolute_fields_raises(self):
        """absolute_expiration and absolute_expiration_relative are mutually exclusive."""
        with pytest.raises(ValueError, match="mutually exclusive"):
            EntryOptions(
                absolute_expiration=datetime(2030, 1, 1, tzinfo=timezone.utc),
                absolute_expiration_relative=timedelta(hours=1),
            )

    def test_naive_absolute_expiration_raises(self):
        """Naive datetimes must be rejected."""
        with pytest.raises(ValueError, match="timezone-aware"):
            EntryOptions(absolute_expiration=datetime(2030, 1, 1))  # no tzinfo


class TestEntryOptionsResolveExpiresAt:
    """Tests for EntryOptions.resolve_expires_at()."""

    _NOW = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_no_absolute_returns_none(self):
        opts = EntryOptions(sliding_expiration=timedelta(minutes=10))
        assert opts.resolve_expires_at(now=self._NOW) is None

    def test_absolute_expiration_returned_as_is(self):
        exp = datetime(2030, 1, 1, tzinfo=timezone.utc)
        opts = EntryOptions(absolute_expiration=exp)
        assert opts.resolve_expires_at(now=self._NOW) == exp

    def test_relative_expiration_added_to_now(self):
        opts = EntryOptions(absolute_expiration_relative=timedelta(hours=1))
        assert opts.resolve_expires_at(now=self._NOW) == self._NOW + timedelta(hours=1)

    def test_no_expiration_returns_none(self):
        opts = EntryOptions()
        assert opts.resolve_expires_at(now=self._NOW) is None

    def test_defaults_to_utc_now_when_no_now_arg(self):
        """resolve_expires_at() without 'now' uses datetime.now(tz=utc)."""
        opts = EntryOptions(absolute_expiration_relative=timedelta(seconds=30))
        result = opts.resolve_expires_at()
        assert result is not None
        assert result.tzinfo is not None  # tz-aware


class TestEntryOptionsSlidingSeconds:
    """Tests for EntryOptions.resolve_sliding_seconds()."""

    def test_none_when_no_sliding(self):
        opts = EntryOptions()
        assert opts.resolve_sliding_seconds() is None

    def test_whole_seconds_integer(self):
        opts = EntryOptions(sliding_expiration=timedelta(minutes=20))
        assert opts.resolve_sliding_seconds() == 1200

    def test_fractional_seconds_truncated(self):
        opts = EntryOptions(sliding_expiration=timedelta(seconds=90, milliseconds=500))
        assert opts.resolve_sliding_seconds() == 90  # int(), not round()
