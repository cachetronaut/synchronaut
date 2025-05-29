import pytest
import asyncio
import time

from synchronaut import synchronaut, call_any, call_map, CallAnyTimeout

# ——— plain functions ———
def sync_add(a, b):
    return a + b

async def async_add(a, b):
    return a + b

# ——— decorated versions ———
@synchronaut()
def dec_sync_add(a, b):
    return a + b

@synchronaut(timeout=1.0)
async def dec_async_add(a, b):
    return a + b

async def main():
    # sync → sync
    print('sync_add:', sync_add(1, 2))
    print('smart_call(sync_add):', await call_any(sync_add, 3, 4))

    # sync → async (in async context, sync funcs auto-offload)
    print('offloaded sync_add:', await call_any(sync_add, 5, 6))

    # async → async
    print('async_add:', await async_add(7, 8))
    print('smart_call(async_add):', await call_any(async_add, 7, 8))

    # batch helper in async
    print('smart_map:', await call_map([sync_add, async_add], 4, 5))

    # decorator shortcuts in async
    print('await dec_sync_add.async_:', await dec_sync_add.async_(6, 7))
    print('await dec_async_add:', await dec_async_add(8, 9))

    # timeout demo (pure-sync offload)
    try:
        await call_any(lambda: time.sleep(2), timeout=0.5)
    except CallAnyTimeout as e:
        print('Timeout caught:', e)

def test_quickstart_sync_paths():
    # sync → sync
    assert sync_add(1, 2) == 3
    assert call_any(sync_add, 3, 4) == 7

    # async → sync
    assert call_any(async_add, 9, 10) == 19

    # decorator passthrough in sync
    assert dec_sync_add(2, 3) == 5
    assert dec_async_add.sync(8, 9) == 17

@pytest.mark.asyncio
async def test_quickstart_async_paths():
    # sync → async
    assert await call_any(sync_add, 5, 6) == 11

    # async → async
    assert await call_any(async_add, 7, 8) == 15

    # batch helper
    assert await call_map([sync_add, async_add], 4, 5) == [9, 9]

    # decorator shortcuts in async
    assert await dec_sync_add.async_(6, 7) == 13
    assert await dec_async_add(8, 9) == 17

    # timeout must raise
    with pytest.raises(CallAnyTimeout):
        await call_any(lambda: time.sleep(2), timeout=0.1)


if __name__ == '__main__':
    # sync-land examples
    print('dec_sync_add(2,3):', dec_sync_add(2, 3))
    print('smart_call(async_add) in sync:', call_any(async_add, 9, 10))
    # then run the async demonstrations
    asyncio.run(main())
