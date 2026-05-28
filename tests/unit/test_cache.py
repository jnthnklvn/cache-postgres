"""
Unit tests for PostgresCache (_cache.py).

Mocks DatabaseOperations to test the public facade, key validation,
scanner thread lifecycle, and context manager — without a real database.

Spec: _reversa_sdd/migration/target_architecture.md § BC-1, DA-03, DA-04
      _reversa_sdd/migration/target_business_rules.md § BR-MIGRAR-001, BR-MIGRAR-005, BR-MIGRAR-012
      _reversa_sdd/migration/risk_register.md § RISK-003
"""

import time
import pytest
from datetime import timedelta
from unittest.mock import MagicMock, patch

from postgres_cache._cache import PostgresCache, _MAX_KEY_LENGTH
from postgres_cache._options import EntryOptions, PostgresCacheOptions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_options(**kwargs) -> PostgresCacheOptions:
    defaults = dict(
        dsn="postgresql://localhost/testdb",
        schema="public",
        table="cache",
        create_if_not_exists=False,
        enable_expiration_scan=False,  # disabled by default in unit tests
    )
    defaults.update(kwargs)
    return PostgresCacheOptions(**defaults)


def make_cache(options: PostgresCacheOptions | None = None) -> tuple[PostgresCache, MagicMock]:
    """Return (PostgresCache, mock_db) with DatabaseOperations mocked."""
    opts = options or make_options()
    cache = PostgresCache(opts)
    mock_db = MagicMock()
    cache._db = mock_db
    return cache, mock_db


# ===========================================================================
# Key validation — BR-MIGRAR-012
# ===========================================================================

class TestKeyValidation:
    def test_empty_key_raises(self):
        cache, _ = make_cache()
        with pytest.raises(ValueError, match="empty"):
            cache.get("")

    def test_key_at_max_length_accepted(self):
        cache, mock_db = make_cache()
        mock_db.get.return_value = None
        key = "x" * _MAX_KEY_LENGTH
        cache.get(key)  # must not raise
        mock_db.get.assert_called_once_with(key)

    def test_key_over_max_length_raises(self):
        cache, _ = make_cache()
        key = "x" * (_MAX_KEY_LENGTH + 1)
        with pytest.raises(ValueError, match="BR-MIGRAR-012"):
            cache.get(key)

    def test_key_validation_applies_to_all_operations(self):
        cache, _ = make_cache()
        bad_key = "k" * (_MAX_KEY_LENGTH + 1)
        for method in (cache.get, cache.refresh, cache.remove):
            with pytest.raises(ValueError):
                method(bad_key)
        with pytest.raises(ValueError):
            cache.set(bad_key, b"value")
        with pytest.raises(ValueError):
            cache.get_or_create(bad_key, lambda: b"v")


# ===========================================================================
# Public API — delegates to DatabaseOperations
# ===========================================================================

class TestGet:
    def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        mock_db.get.return_value = b"result"
        assert cache.get("key") == b"result"
        mock_db.get.assert_called_once_with("key")

    def test_returns_none_on_miss(self):
        cache, mock_db = make_cache()
        mock_db.get.return_value = None
        assert cache.get("key") is None


class TestSet:
    def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        opts = EntryOptions(sliding_expiration=timedelta(minutes=10))
        cache.set("key", b"value", opts)
        mock_db.set.assert_called_once_with("key", b"value", opts)

    def test_delegates_without_options(self):
        cache, mock_db = make_cache()
        cache.set("key", b"value")
        mock_db.set.assert_called_once_with("key", b"value", None)


class TestRefresh:
    def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        cache.refresh("key")
        mock_db.refresh.assert_called_once_with("key")


class TestRemove:
    def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        cache.remove("key")
        mock_db.remove.assert_called_once_with("key")


class TestGetOrCreate:
    def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        factory = lambda: b"computed"
        mock_db.get_or_create.return_value = b"computed"
        result = cache.get_or_create("key", factory)
        assert result == b"computed"
        mock_db.get_or_create.assert_called_once_with("key", factory, None)

    def test_passes_options_to_db(self):
        cache, mock_db = make_cache()
        opts = EntryOptions(sliding_expiration=timedelta(minutes=5))
        mock_db.get_or_create.return_value = b"val"
        cache.get_or_create("key", lambda: b"val", opts)
        mock_db.get_or_create.assert_called_once_with("key", mock_db.get_or_create.call_args[0][1], opts)


# ===========================================================================
# Bulk Operations
# ===========================================================================

class TestBulkOperations:
    def test_get_many_delegates_to_db(self):
        cache, mock_db = make_cache()
        mock_db.get_many.return_value = {"k1": b"v1"}
        assert cache.get_many(["k1", "k2"]) == {"k1": b"v1"}
        mock_db.get_many.assert_called_once_with(["k1", "k2"])

    def test_set_many_delegates_to_db(self):
        cache, mock_db = make_cache()
        opts = EntryOptions(sliding_expiration=timedelta(minutes=10))
        cache.set_many({"k1": b"v1"}, opts)
        mock_db.set_many.assert_called_once_with({"k1": b"v1"}, opts)

    def test_delete_many_delegates_to_db(self):
        cache, mock_db = make_cache()
        cache.delete_many(["k1", "k2"])
        mock_db.delete_many.assert_called_once_with(["k1", "k2"])

    def test_key_validation_in_bulk(self):
        cache, _ = make_cache()
        bad_key = "x" * (_MAX_KEY_LENGTH + 1)
        with pytest.raises(ValueError):
            cache.get_many(["ok", bad_key])
        with pytest.raises(ValueError):
            cache.set_many({"ok": b"v", bad_key: b"v"})
        with pytest.raises(ValueError):
            cache.delete_many(["ok", bad_key])


# ===========================================================================
# Pattern Matching
# ===========================================================================

class TestPatternMatching:
    def test_get_pattern_delegates_to_db(self):
        cache, mock_db = make_cache()
        mock_db.get_pattern.return_value = {"user:1": b"v1"}
        assert cache.get_pattern("user:%") == {"user:1": b"v1"}
        mock_db.get_pattern.assert_called_once_with("user:%")

    def test_delete_pattern_delegates_to_db(self):
        cache, mock_db = make_cache()
        mock_db.delete_pattern.return_value = 5
        assert cache.delete_pattern("user:%") == 5
        mock_db.delete_pattern.assert_called_once_with("user:%")

    def test_empty_pattern_raises(self):
        cache, _ = make_cache()
        with pytest.raises(ValueError, match="empty"):
            cache.get_pattern("")
        with pytest.raises(ValueError, match="empty"):
            cache.delete_pattern("")


# ===========================================================================
# Context manager — RISK-003, DA-04
# ===========================================================================

class TestContextManager:
    def test_enter_returns_self(self):
        cache, _ = make_cache()
        result = cache.__enter__()
        cache.close()
        assert result is cache

    def test_exit_calls_close(self):
        cache, _ = make_cache()
        cache.__enter__()
        with patch.object(cache, "close") as mock_close:
            cache.__exit__(None, None, None)
        mock_close.assert_called_once()

    def test_context_manager_protocol(self):
        opts = make_options(enable_expiration_scan=False)
        with PostgresCache(opts) as cache:
            cache._db = MagicMock()
            cache._db.get.return_value = b"hi"
            assert cache.get("k") == b"hi"

    def test_exit_does_not_suppress_exceptions(self):
        opts = make_options()
        cache = PostgresCache(opts)
        result = cache.__exit__(ValueError, ValueError("oops"), None)
        assert result is None  # must not suppress

    def test_close_is_idempotent(self):
        cache, _ = make_cache()
        cache.close()
        cache.close()  # must not raise or error


# ===========================================================================
# Scanner thread — BR-MIGRAR-005, DA-03, RISK-003
# ===========================================================================

class TestScannerThread:
    def test_scanner_not_started_when_disabled(self):
        opts = make_options(enable_expiration_scan=False)
        cache = PostgresCache(opts)
        cache._db = MagicMock()
        cache.__enter__()
        assert cache._scanner_thread is None
        cache.close()

    def test_scanner_started_on_enter(self):
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(hours=1),  # long interval — won't fire
        )
        cache = PostgresCache(opts)
        cache._db = MagicMock()
        cache.__enter__()
        try:
            assert cache._scanner_thread is not None
            assert cache._scanner_thread.is_alive()
        finally:
            cache.close()

    def test_scanner_thread_is_daemon(self):
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(hours=1),
        )
        cache = PostgresCache(opts)
        cache._db = MagicMock()
        cache.__enter__()
        try:
            assert cache._scanner_thread.daemon is True  # DA-03
        finally:
            cache.close()

    def test_scanner_stops_on_close(self):
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(hours=1),
        )
        cache = PostgresCache(opts)
        cache._db = MagicMock()
        cache.__enter__()
        thread = cache._scanner_thread
        assert thread.is_alive()
        cache.close()
        # Give thread up to 2 s to terminate
        thread.join(timeout=2.0)
        assert not thread.is_alive(), "Scanner thread did not stop after close()"

    def test_scanner_calls_delete_expired(self):
        """Scanner must invoke delete_expired at the configured interval."""
        # Use a valid options (5-min floor) but then override _options on the
        # already-constructed instance so the scanner runs fast in the test.
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(minutes=5),  # valid for construction
        )
        cache = PostgresCache(opts)
        mock_db = MagicMock()
        mock_db.delete_expired.return_value = 0
        cache._db = mock_db

        # Override the interval to something fast, bypassing construction validation.
        # object.__setattr__ skips __post_init__ (dataclasses are mutable by default).
        object.__setattr__(
            cache._options,
            "expiration_scan_interval",
            timedelta(milliseconds=50),
        )

        cache._start_scanner()
        time.sleep(0.35)  # allow at least 3 scan cycles at 50ms
        cache.close()

        assert mock_db.delete_expired.call_count >= 2, (
            f"Expected >= 2 delete_expired calls, got {mock_db.delete_expired.call_count}"
        )

    def test_scanner_continues_after_error(self):
        """Scanner must not crash the thread on a transient error — BR-MIGRAR-011."""
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(minutes=5),  # valid for construction
        )
        cache = PostgresCache(opts)
        mock_db = MagicMock()
        call_count = {"n": 0}

        def delete_expired_side_effect():
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("transient db error")
            return 0

        mock_db.delete_expired.side_effect = delete_expired_side_effect
        cache._db = mock_db

        # Override interval to fast value, bypassing construction validation.
        object.__setattr__(
            cache._options,
            "expiration_scan_interval",
            timedelta(milliseconds=50),
        )

        cache._start_scanner()
        time.sleep(0.35)
        cache.close()

        # Thread should have continued after the first error and made more calls
        assert call_count["n"] >= 2, (
            f"Expected >= 2 calls after transient error, got {call_count['n']}"
        )
        assert cache._scanner_thread is None or not (
            cache._scanner_thread and cache._scanner_thread.is_alive()
        ), "Scanner thread should be stopped after close()"

    def test_start_scanner_is_idempotent(self):
        """Calling _start_scanner twice must not spawn two threads."""
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(hours=1),
        )
        cache = PostgresCache(opts)
        cache._db = MagicMock()
        cache._start_scanner()
        thread1 = cache._scanner_thread
        cache._start_scanner()  # second call — must be no-op
        thread2 = cache._scanner_thread
        assert thread1 is thread2, "Second _start_scanner() spawned a new thread"
        cache.close()
