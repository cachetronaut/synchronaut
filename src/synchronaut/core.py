import asyncio
import inspect
import threading
from functools import partial
from typing import Callable, Any
from concurrent.futures import (
    ThreadPoolExecutor, 
    TimeoutError as FutureTimeoutError
)

from anyio import to_thread

# ——— background loop for reuse ——
_background_loop: asyncio.AbstractEventLoop | None = None
_background_thread: threading.Thread | None = None

def _start_background_loop() -> asyncio.AbstractEventLoop:
    global _background_loop, _background_thread
    if _background_loop is None:
        _background_loop = asyncio.new_event_loop()
        def _run_loop():
            asyncio.set_event_loop(_background_loop)
            _background_loop.run_forever()
        _background_thread = threading.Thread(target=_run_loop, daemon=True)
        _background_thread.start()
    return _background_loop

def _offload_sync(
    func: Callable, 
    /, 
    *args, 
    executor: ThreadPoolExecutor | None = None, 
    timeout: float | None = None
) -> Any:
    '''
    Always submit `func(*args)` to a single, long-lived thread-pool
    running on our background event loop, and convert timeouts.
    '''
    loop = _start_background_loop()
    coro = to_thread.run_sync(
        partial(func, *args), abandon_on_cancel=True
    )
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return future.result(timeout)
    except FutureTimeoutError as e:
        raise CallAnyTimeout(
            f'Function {func.__name__} timed out after {timeout}s'
        ) from e

# ——— check if async ——
def _in_async_context() -> bool:
    '''
    Return True if we're already inside an asyncio or Trio event loop.
    '''
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        pass

    try:
        import trio
        return trio.lowlevel.current_trio_token() is not None
    except Exception:
        return False

class CallAnyTimeout(Exception):
    '''Raised when call_any exceeds the given timeout.'''

def call_any(
    func: Callable,
    *args,
    timeout: float | None = None,
    executor: ThreadPoolExecutor | None = None,
    force_offload: bool = False,
    reuse_loop: bool = False,
    **kwargs
) -> Any:
    '''
    Call a sync or async function seamlessly from sync or async context.

    - timeout : seconds before cancel/timeout
    - executor : custom ThreadPoolExecutor for offload
    - force_offload : in sync-land, offload even pure-sync funcs
    - reuse_loop : in sync-land coroutines, submit to a background loop
    '''
    is_coro = inspect.iscoroutinefunction(func)

    # helper to run inside anyio / asyncio loop
    async def _run_inside_async():
        if is_coro:
            return await func(*args, **kwargs)
        return await to_thread.run_sync(partial(
            func, *args, **kwargs), abandon_on_cancel=True
        )

    # ——— async-land — return a coroutine that callers must await
    if _in_async_context():
        if timeout is not None:
            async def _with_timeout():
                try:
                    return await asyncio.wait_for(_run_inside_async(), timeout)
                except asyncio.TimeoutError as e:
                    raise CallAnyTimeout(
                        f'Function {func.__name__} timed out after {timeout}s'
                    ) from e
            return _with_timeout()
        return _run_inside_async()

    # ——— sync-land ——
    if is_coro:
        coro = func(*args, **kwargs)
        if reuse_loop:
            future = asyncio.run_coroutine_threadsafe(
                coro, _start_background_loop()
            )
            try:
                return future.result(timeout)
            except FutureTimeoutError as e:
                raise CallAnyTimeout(
                    f'Function {func.__name__} timed out after {timeout}s'
                ) from e

        # normal single-shot loop, with optional timeout
        if timeout is not None:
            try:
                return asyncio.run(asyncio.wait_for(coro, timeout))
            except asyncio.TimeoutError as e:
                raise CallAnyTimeout(
                    f'Function {func.__name__} timed out after {timeout}s'
                ) from e
        return asyncio.run(coro)

    # ——— pure-sync function ——
    if force_offload or timeout is not None:
        return _offload_sync(func, *args, executor=executor, timeout=timeout)

    # default direct call
    return func(*args, **kwargs)

def call_map(
    funcs: list[Callable],
    *args,
    timeout: float | None = None,
    executor: ThreadPoolExecutor | None = None,
    **kwargs
) -> Any:
    '''
    Run multiple sync/async funcs in parallel when in async context,
    or sequentially in sync context.
    '''
    if _in_async_context():
        coros = [
            call_any(f, *args, timeout=timeout, executor=executor, **kwargs)
            for f in funcs
        ]
        return asyncio.gather(*coros)
    return [
        call_any(f, *args, timeout=timeout, executor=executor, **kwargs)
        for f in funcs
    ]
