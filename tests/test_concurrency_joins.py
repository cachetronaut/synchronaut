import asyncio
import pytest

from synchronaut.utils import get_preferred_loop, is_uvloop, has_uvloop_policy

def test_has_uvloop_policy_can_detect_default(monkeypatch):
    # simulate a non-uvloop policy
    class DummyPolicy: ...
    monkeypatch.setattr(asyncio, 'get_event_loop_policy', lambda: DummyPolicy())
    assert not has_uvloop_policy()

def test_has_uvloop_policy_detects_uvloop():
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    assert has_uvloop_policy()

def test_get_preferred_loop_sync_sets_and_returns_loop(tmp_path):
    # in sync code, this should set and return a thread-local loop
    loop = get_preferred_loop()
    assert isinstance(loop, asyncio.AbstractEventLoop)
    # subsequent get_event_loop() must return the same
    assert asyncio.get_event_loop() is loop

@pytest.mark.asyncio
async def test_get_preferred_loop_async_returns_running_loop():
    # inside async def, it must return get_running_loop()
    rl = asyncio.get_running_loop()
    pl = get_preferred_loop()
    assert pl is rl

@pytest.mark.asyncio
async def test_is_uvloop_switches_with_policy():
    # install uvloop policy
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    rl = asyncio.get_running_loop()
    assert is_uvloop(rl)
    # revert to default policy
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    non_uv = get_preferred_loop()
    assert not is_uvloop(non_uv)