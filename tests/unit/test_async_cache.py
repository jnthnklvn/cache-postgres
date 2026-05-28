"""
Unit tests for AsyncPostgresCache (_async_cache.py).
"""

import asyncio
import pytest
from datetime import timedelta
from unittest.mock import AsyncMock, patch, MagicMock

from postgres_cache._async_cache import AsyncPostgresCache, _MAX_KEY_LENGTH
from postgres_cache._options import EntryOptions, PostgresCacheOptions


@pytest.fixture(scope="module", params=["asyncio"])
def anyio_backend(request):
    return request.param


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


def make_cache(options: PostgresCacheOptions | None = None) -> tuple[AsyncPostgresCache, AsyncMock]:
    """Return (AsyncPostgresCache, mock_db) with AsyncDatabaseOperations mocked."""
    opts = options or make_options()
    cache = AsyncPostgresCache(opts)
    mock_db = AsyncMock()
    cache._db = mock_db
    return cache, mock_db


# ===========================================================================
# Key validation
# ===========================================================================

class TestKeyValidation:
    @pytest.mark.anyio
    async def test_empty_key_raises(self):
        cache, _ = make_cache()
        with pytest.raises(ValueError, match="empty"):
            await cache.get("")

    @pytest.mark.anyio
    async def test_key_at_max_length_accepted(self):
        cache, mock_db = make_cache()
        mock_db.get.return_value = None
        key = "x" * _MAX_KEY_LENGTH
        await cache.get(key)  # must not raise
        mock_db.get.assert_called_once_with(key)

    @pytest.mark.anyio
    async def test_key_over_max_length_raises(self):
        cache, _ = make_cache()
        key = "x" * (_MAX_KEY_LENGTH + 1)
        with pytest.raises(ValueError, match="exceeds maximum length"):
            await cache.get(key)

    @pytest.mark.anyio
    async def test_key_validation_applies_to_all_operations(self):
        cache, _ = make_cache()
        bad_key = "k" * (_MAX_KEY_LENGTH + 1)
        
        with pytest.raises(ValueError):
            await cache.get(bad_key)
        with pytest.raises(ValueError):
            await cache.refresh(bad_key)
        with pytest.raises(ValueError):
            await cache.remove(bad_key)
        with pytest.raises(ValueError):
            await cache.set(bad_key, b"value")
        with pytest.raises(ValueError):
            async def factory(): return b"v"
            await cache.get_or_create(bad_key, factory)


# ===========================================================================
# Public API — delegates to AsyncDatabaseOperations
# ===========================================================================

class TestGet:
    @pytest.mark.anyio
    async def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        mock_db.get.return_value = b"result"
        assert await cache.get("key") == b"result"
        mock_db.get.assert_called_once_with("key")

    @pytest.mark.anyio
    async def test_returns_none_on_miss(self):
        cache, mock_db = make_cache()
        mock_db.get.return_value = None
        assert await cache.get("key") is None


class TestSet:
    @pytest.mark.anyio
    async def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        opts = EntryOptions(sliding_expiration=timedelta(minutes=10))
        await cache.set("key", b"value", opts)
        mock_db.set.assert_called_once_with("key", b"value", opts)

    @pytest.mark.anyio
    async def test_delegates_without_options(self):
        cache, mock_db = make_cache()
        await cache.set("key", b"value")
        mock_db.set.assert_called_once_with("key", b"value", None)


class TestRefresh:
    @pytest.mark.anyio
    async def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        await cache.refresh("key")
        mock_db.refresh.assert_called_once_with("key")


class TestRemove:
    @pytest.mark.anyio
    async def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        await cache.remove("key")
        mock_db.remove.assert_called_once_with("key")


class TestDeleteTags:
    @pytest.mark.anyio
    async def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        await cache.delete_tags("tag1", "tag2")
        mock_db.delete_by_tags.assert_called_once_with(["tag1", "tag2"])


class TestGetOrCreate:
    @pytest.mark.anyio
    async def test_delegates_to_db(self):
        cache, mock_db = make_cache()
        async def factory(): return b"computed"
        mock_db.get_or_create.return_value = b"computed"
        result = await cache.get_or_create("key", factory)
        assert result == b"computed"
        mock_db.get_or_create.assert_called_once_with("key", factory, None)

    @pytest.mark.anyio
    async def test_passes_options_to_db(self):
        cache, mock_db = make_cache()
        opts = EntryOptions(sliding_expiration=timedelta(minutes=5))
        mock_db.get_or_create.return_value = b"val"
        async def factory(): return b"val"
        await cache.get_or_create("key", factory, opts)
        mock_db.get_or_create.assert_called_once_with("key", factory, opts)


# ===========================================================================
# Bulk Operations
# ===========================================================================

class TestBulkOperations:
    @pytest.mark.anyio
    async def test_get_many_delegates_to_db(self):
        cache, mock_db = make_cache()
        mock_db.get_many.return_value = {"k1": b"v1"}
        assert await cache.get_many(["k1", "k2"]) == {"k1": b"v1"}
        mock_db.get_many.assert_called_once_with(["k1", "k2"])

    @pytest.mark.anyio
    async def test_set_many_delegates_to_db(self):
        cache, mock_db = make_cache()
        opts = EntryOptions(sliding_expiration=timedelta(minutes=10))
        await cache.set_many({"k1": b"v1"}, opts)
        mock_db.set_many.assert_called_once_with({"k1": b"v1"}, opts)

    @pytest.mark.anyio
    async def test_delete_many_delegates_to_db(self):
        cache, mock_db = make_cache()
        await cache.delete_many(["k1", "k2"])
        mock_db.delete_many.assert_called_once_with(["k1", "k2"])

    @pytest.mark.anyio
    async def test_key_validation_in_bulk(self):
        cache, _ = make_cache()
        bad_key = "x" * (_MAX_KEY_LENGTH + 1)
        with pytest.raises(ValueError):
            await cache.get_many(["ok", bad_key])
        with pytest.raises(ValueError):
            await cache.set_many({"ok": b"v", bad_key: b"v"})
        with pytest.raises(ValueError):
            await cache.delete_many(["ok", bad_key])


# ===========================================================================
# Pattern Matching
# ===========================================================================

class TestPatternMatching:
    @pytest.mark.anyio
    async def test_get_pattern_delegates_to_db(self):
        cache, mock_db = make_cache()
        mock_db.get_pattern.return_value = {"user:1": b"v1"}
        assert await cache.get_pattern("user:%") == {"user:1": b"v1"}
        mock_db.get_pattern.assert_called_once_with("user:%")

    @pytest.mark.anyio
    async def test_delete_pattern_delegates_to_db(self):
        cache, mock_db = make_cache()
        mock_db.delete_pattern.return_value = 5
        assert await cache.delete_pattern("user:%") == 5
        mock_db.delete_pattern.assert_called_once_with("user:%")

    @pytest.mark.anyio
    async def test_empty_pattern_raises(self):
        cache, _ = make_cache()
        with pytest.raises(ValueError, match="empty"):
            await cache.get_pattern("")
        with pytest.raises(ValueError, match="empty"):
            await cache.delete_pattern("")


# ===========================================================================
# Context manager
# ===========================================================================

class TestContextManager:
    @pytest.mark.anyio
    async def test_enter_returns_self(self):
        cache, _ = make_cache()
        result = await cache.__aenter__()
        await cache.close()
        assert result is cache

    @pytest.mark.anyio
    async def test_exit_calls_close(self):
        cache, _ = make_cache()
        await cache.__aenter__()
        with patch.object(cache, "close", new_callable=AsyncMock) as mock_close:
            await cache.__aexit__(None, None, None)
        mock_close.assert_called_once()

    @pytest.mark.anyio
    async def test_close_is_idempotent(self):
        cache, _ = make_cache()
        await cache.close()
        await cache.close()  # must not raise or error


# ===========================================================================
# Scanner Task
# ===========================================================================

class TestScannerTask:
    @pytest.mark.anyio
    async def test_scanner_not_started_when_disabled(self):
        opts = make_options(enable_expiration_scan=False)
        cache = AsyncPostgresCache(opts)
        cache._db = AsyncMock()
        await cache.__aenter__()
        assert cache._scanner_task is None
        await cache.close()

    @pytest.mark.anyio
    async def test_scanner_started_on_enter(self):
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(hours=1),
        )
        cache = AsyncPostgresCache(opts)
        cache._db = AsyncMock()
        await cache.__aenter__()
        try:
            assert cache._scanner_task is not None
            assert not cache._scanner_task.done()
        finally:
            await cache.close()

    @pytest.mark.anyio
    async def test_scanner_stops_on_close(self):
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(hours=1),
        )
        cache = AsyncPostgresCache(opts)
        cache._db = AsyncMock()
        await cache.__aenter__()
        task = cache._scanner_task
        assert not task.done()
        await cache.close()
        assert task.cancelled() or task.done()

    @pytest.mark.anyio
    async def test_scanner_calls_delete_expired(self):
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(minutes=5),  # valid
        )
        cache = AsyncPostgresCache(opts)
        mock_db = AsyncMock()
        mock_db.delete_expired.return_value = 0
        cache._db = mock_db

        # Override interval fast
        object.__setattr__(cache._options, "expiration_scan_interval", timedelta(milliseconds=50))

        cache._start_scanner()
        await asyncio.sleep(0.35)
        await cache.close()

        assert mock_db.delete_expired.call_count >= 2

    @pytest.mark.anyio
    async def test_scanner_continues_after_error(self):
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(minutes=5),
        )
        cache = AsyncPostgresCache(opts)
        mock_db = AsyncMock()
        call_count = {"n": 0}

        async def delete_expired_side_effect():
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("transient db error")
            return 0

        mock_db.delete_expired.side_effect = delete_expired_side_effect
        cache._db = mock_db

        object.__setattr__(cache._options, "expiration_scan_interval", timedelta(milliseconds=50))

        cache._start_scanner()
        await asyncio.sleep(0.35)
        await cache.close()

        assert call_count["n"] >= 2
        assert cache._scanner_task is None or cache._scanner_task.done()

    @pytest.mark.anyio
    async def test_start_scanner_is_idempotent(self):
        opts = make_options(
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(hours=1),
        )
        cache = AsyncPostgresCache(opts)
        cache._db = AsyncMock()
        cache._start_scanner()
        task1 = cache._scanner_task
        cache._start_scanner()  # second call
        task2 = cache._scanner_task
        assert task1 is task2
        await cache.close()
