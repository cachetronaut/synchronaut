import pytest
import asyncio
import trio

from synchronaut import parallel_map, CallAnyTimeout

# Synchronous helper functions
def sync_greet(name):
    return f'Hello, {name}!'

def sync_add(a, b):
    return a + b

def sync_sleep_and_return(x):
    import time
    time.sleep(x)
    return f'Slept {x}s'

# Asynchronous helper for asyncio
async def async_hello(name):
    await asyncio.sleep(0.1)
    return f'Hi, {name}!'

def sync_double(x):
    return x * 2

# Asynchronous helper for trio
async def async_square(n):
    await trio.sleep(0.05)
    return n * n

def sync_subtract(a, b):
    return a - b


def test_sync_functions():
    '''
    Test parallel_map with purely synchronous callables, including a timeout.
    '''
    calls = [
        (sync_greet, ('Alice',), {}, None),
        (sync_add, (2, 3), {}, 1.0),
        (sync_sleep_and_return, (2,), {}, 1.0),
    ]

    results = parallel_map(calls, return_exceptions=True)

    assert results[0] == 'Hello, Alice!'
    assert results[1] == 5
    assert isinstance(results[2], CallAnyTimeout)


@pytest.mark.asyncio
async def test_asyncio_functions():
    '''
    Test parallel_map in an asyncio context, mixing async and sync callables, including a forced timeout.
    '''
    calls = [
        # run async_hello('Charlie') with a 0.5s timeout
        (async_hello, ('Charlie',), {}, 0.5),

        # run sync_double(21) with no timeout
        (sync_double, (21,), {}, None),

        # run async_hello but force it to “time out” in 0.01s
        (async_hello, ('TooSlow',), {}, 0.01),
    ]

    results = await parallel_map(calls, return_exceptions=True)

    assert results[0] == 'Hi, Charlie!'
    assert results[1] == 42
    assert isinstance(results[2], CallAnyTimeout)


@pytest.mark.trio
async def test_trio_functions():
    '''
    Test parallel_map in a Trio context, mixing async and sync callables, including a forced timeout.
    '''
    calls = [
        (async_square, (5,),   {}, 0.1),   # finishes in 0.05s < 0.1s
        (sync_subtract,  (10, 3), {}, None),
        (async_square, (10,),  {}, 0.01),  # will timeout
    ]

    results = await parallel_map(calls, return_exceptions=True)

    assert results[0] == 25
    assert results[1] == 7
    assert isinstance(results[2], CallAnyTimeout)