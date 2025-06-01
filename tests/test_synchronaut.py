import pytest
import asyncio
import time
from synchronaut.core import call_any, call_map, CallAnyTimeout
from synchronaut.synchronaut import synchronaut

# ––– Sample functions ––

def sync_add(a, b):
    return a + b

async def async_add(a, b):
    return a + b

def long_sync():
    time.sleep(0.1)
    return 'done'

async def long_async():
    await asyncio.sleep(0.1)
    return 'done'

# ––– Four quadrants for smart_call ––

def test_sync_to_sync():
    assert call_any(sync_add, 1, 2) == 3

@pytest.mark.asyncio
async def test_sync_to_async():
    # In async context, sync funcs are offloaded
    result = await call_any(sync_add, 3, 4)
    assert result == 7

@pytest.mark.asyncio
async def test_async_to_async():
    result = await call_any(async_add, 5, 6)
    assert result == 11

def test_async_to_sync():
    # In sync context, async funcs are run to completion
    assert call_any(async_add, 7, 8) == 15

def test_async_to_sync_with_reuse_loop():
    assert call_any(async_add, 9, 10) == 19

# ––– Timeout behavior ––

def test_sync_timeout():
    with pytest.raises(CallAnyTimeout):
        call_any(long_sync, timeout=0.01)

def test_async_timeout_in_sync():
    with pytest.raises(CallAnyTimeout):
        call_any(long_async, timeout=0.01)

@pytest.mark.asyncio
async def test_async_timeout_in_async():
    with pytest.raises(CallAnyTimeout):
        await call_any(long_async, timeout=0.01)

# ––– smart_map behavior ––

def test_smart_map_sync():
    funcs = [lambda x: x * 2, lambda x: x + 3]
    assert call_map(funcs, 5) == [10, 8]

@pytest.mark.asyncio
async def test_smart_map_async():
    funcs = [sync_add, async_add]
    results = await call_map(funcs, 2, 3)
    assert results == [5, 5]

# ––– Decorator API ––

def test_decorator_on_sync_function():
    @synchronaut()
    def mul(a, b):
        return a * b

    assert mul(4, 5) == 20
    assert hasattr(mul, 'async_')
    assert asyncio.iscoroutinefunction(mul.async_)
    # Call the async_ version in sync–land via asyncio.run
    assert asyncio.run(mul.async_(6, 7)) == 42

@pytest.mark.asyncio
async def test_decorator_on_async_function():
    @synchronaut()
    async def sub(a, b):
        return a - b

    # In async context, calling the wrapper returns a coroutine
    result = await sub(10, 3)
    assert result == 7
    # The .sync bypass should work in sync–land
    assert sub.sync(8, 2) == 6