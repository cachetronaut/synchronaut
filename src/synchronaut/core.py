import asyncio
import inspect
import threading

from functools import partial, lru_cache
from typing import Any, Callable
from concurrent.futures import (
    ThreadPoolExecutor, 
    TimeoutError as FutureTimeoutError
)

import anyio
import trio

# ─── If uvloop is installed, make it the default asyncio event loop ───
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# ─── Shared Global Resources ───
_background_loop: asyncio.AbstractEventLoop | None = None
_background_thread: threading.Thread | None = None
_SHARED_EXECUTOR = ThreadPoolExecutor(max_workers=4)

def _start_background_loop() -> asyncio.AbstractEventLoop:
    '''
    Lazily start one background asyncio loop in its own daemon thread.
    All `run_coroutine_threadsafe(...)` calls will go here. Returns that loop.
    '''
    global _background_loop, _background_thread
    if _background_loop is None:
        loop = asyncio.new_event_loop()
        _background_loop = loop

        def _run_loop_forever():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=_run_loop_forever, daemon=True)
        thread.start()
        _background_thread = thread

    return _background_loop

@lru_cache(maxsize=None)
def _is_coro_fn(fn: Callable) -> bool:
    '''
    Returns True if `fn` is declared as `async def`. Cached for speed.
    '''
    return inspect.iscoroutinefunction(fn)

def _in_async_context() -> str | None:
    '''
    Detect if we are currently inside an asyncio loop or a Trio loop.
    Returns:
      - 'asyncio' if asyncio.get_running_loop() succeeds,
      - 'trio' if trio.lowlevel.current_trio_token() is non-None,
      - None if neither.
    '''
    try:
        asyncio.get_running_loop()
        return 'asyncio'
    except RuntimeError:
        pass

    try:
        # If this does not raise, and returns non-None, we’re in Trio.
        token = trio.lowlevel.current_trio_token()
        return 'trio' if (token is not None) else None
    except Exception:
        return None

class CallAnyTimeout(Exception):
    '''Raised when call_any(...) exceeds the given timeout.'''
    pass

def call_any(
    func: Callable,
    *args,
    timeout: float | None = None,
    executor: ThreadPoolExecutor | None = None,
    force_offload: bool = False,
    **kwargs
) -> Any:
    '''
    Call a sync or async function seamlessly from either sync- or async-contexts,
    with optional timeouts, offloading, and custom executors. `uvloop` will be
    used whenever `asyncio` is in play for faster task scheduling.

    Parameters:
    - `func`: a sync def or async def to invoke
    - `*args`,`**kwargs`: passed to func
    - `timeout`: if not `None`, maximum seconds to wait before raising `CallAnyTimeout`
    - `executor`: optional `ThreadPoolExecutor` for offloading a pure‐sync 
      function (if not provided, defaults to our module’s `_SHARED_EXECUTOR`).
    - `force_offload`: if True (in *sync* context), force a sync function to run 
      in a thread, even if timeout is not set. That allows timely cancellation.

    Returns:
    - “Async-land"
        - If called from inside an asyncio or Trio loop:
            - If `func` is `async def`: returns a coroutine that the caller must `await`.
            - If `func` is `def`, offloads it into a thread (via `anyio.to_thread.run_sync`) 
            and returns a coroutine to await.
        - In both cases, if `timeout` is set, we wrap with a failsafe so that exceeding 
        the timeout raises CallAnyTimeout.
    - “Sync-land"
        - If called from plain sync code:
            - If `func` is `async def`, we schedule it on our single background asyncio loop 
            via `asyncio.run_coroutine_threadsafe(...)` and block on `.result(timeout)`. 
            If the timeout expires, we raise `CallAnyTimeout`.
            - If `func` is `def` and (`force_offload` is `True` or `timeout` is not `None`):
                - If a caller‐supplied `executor` is provided, we do a direct 
                `executor.submit(func,…)` with `.result(timeout)`. 
                - Otherwise, we package it as a small coroutine that calls 
                `anyio.to_thread.run_sync(func)`, schedule that coroutine on our background 
                loop, and block on `.result(timeout)`.  
                - Either way, on expiry we raise `CallAnyTimeout`.
    - Otherwise, we just call `func(*args,**kwargs)` directly (blocking the current thread).
    '''
    is_coro = _is_coro_fn(func)
    mode = _in_async_context()

    # ─── Async-land ───
    if mode == 'asyncio':
        loop = asyncio.get_running_loop()

        if is_coro:
            # An `async def` under asyncio:
            if timeout is not None:
                async def _aio_with_timeout():
                    try:
                        return await asyncio.wait_for(func(*args, **kwargs), timeout)
                    except asyncio.TimeoutError as e:
                        raise CallAnyTimeout(
                            f'Function {func.__name__} timed out after {timeout}s'
                        ) from e
                return _aio_with_timeout()
            # No timeout—just hand back the coroutine for the caller to await:
            return func(*args, **kwargs)

        # A plain‐sync function (def) under asyncio:
        target_exec = executor if (executor is not None) else _SHARED_EXECUTOR

        if timeout is not None:
            async def _aio_offload_with_timeout():
                try:
                    return await asyncio.wait_for(
                        loop.run_in_executor(
                            target_exec, partial(func, *args, **kwargs)
                        ),
                        timeout
                    )
                except asyncio.TimeoutError as e:
                    raise CallAnyTimeout(
                        f'Function {func.__name__} timed out after {timeout}s'
                    ) from e
            return _aio_offload_with_timeout()

        if force_offload:
            # Offload to the chosen executor but no timeout:
            return loop.run_in_executor(
                target_exec, partial(func, *args, **kwargs)
            )

        # If no timeout and no force_offload, just run it “inline” as a coroutine that blocks:
        async def _aio_direct():
            return func(*args, **kwargs)
        return _aio_direct()

    elif mode == 'trio':
        # We’re inside a Trio run loop—use Trio + anyio primitives:
        if is_coro:
            # An `async def` under Trio:
            if timeout is not None:
                async def _trio_with_timeout():
                    try:
                        # anyio.fail_after(...) uses trio.fail_after(...) here
                        with anyio.fail_after(timeout):
                            return await func(*args, **kwargs)
                    except Exception:
                        # anyio.fail_after will raise a Trio Cancelled/TooSlow error
                        raise CallAnyTimeout(
                            f'Function {func.__name__} timed out after {timeout}s'
                        )
                return _trio_with_timeout()
            # No timeout—just hand back the coroutine for the caller to await:
            return func(*args, **kwargs)

        # A plain‐sync function (def) under Trio:
        if timeout is not None:
            async def _trio_offload_with_timeout():
                try:
                    with anyio.fail_after(timeout):
                        return await anyio.to_thread.run_sync(
                            partial(func, *args, **kwargs), cancellable=True
                        )
                except Exception:
                    raise CallAnyTimeout(
                        f'Function {func.__name__} timed out after {timeout}s'
                    )
            return _trio_offload_with_timeout()

        if force_offload:
            async def _trio_offload():
                return await anyio.to_thread.run_sync(
                    partial(func, *args, **kwargs), cancellable=True
                )
            return _trio_offload()

        # No timeout, no force_offload—run inline (blocking) as a coroutine:
        async def _trio_direct():
            return func(*args, **kwargs)
        return _trio_direct()

    # ─── Sync-land ───
    # a) If `func` is an `async def`, schedule it on the background asyncio loop
    if is_coro:
        coro = func(*args, **kwargs)
        loop = _start_background_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            return future.result(timeout)
        except FutureTimeoutError as e:
            raise CallAnyTimeout(
                f'Coroutine {func.__name__} timed out after {timeout}s'
            ) from e

    # b) `func` is a plain‐sync function (`def`)
    if force_offload or (timeout is not None):
        # If a caller provided their own executor, use it directly:
        if executor is not None:
            future2 = executor.submit(func, *args, **kwargs)
            try:
                return future2.result(timeout)
            except FutureTimeoutError as e:
                raise CallAnyTimeout(
                    f'Function {func.__name__} timed out after {timeout}s'
                ) from e

        # Otherwise, wrap it in a tiny coroutine + anyio.to_thread, schedule on
        # the background asyncio loop, and block on .result(timeout):
        async def _offload():
            return await anyio.to_thread.run_sync(
                partial(func, *args, **kwargs),
                abandon_on_cancel=True
            )

        loop = _start_background_loop()
        future = asyncio.run_coroutine_threadsafe(_offload(), loop)
        try:
            return future.result(timeout)
        except FutureTimeoutError as e:
            raise CallAnyTimeout(
                f'Function {func.__name__} timed out after {timeout}s'
            ) from e

    # ─── Nothing-land ───
    # c) No offload, no timeout—just call it directly
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

    - If in asyncio: returns `asyncio.gather(...)` on all call_any(...) coroutines.
    - If in Trio: returns a Trio coroutine that calls each one with `await` in a list.
    - Otherwise (plain sync): returns a normal Python list of `call_any(...)` results.
    '''
    mode = _in_async_context()
    if mode == 'asyncio':
        return asyncio.gather(
            *(call_any(
                    f, *args, timeout=timeout, executor=executor, **kwargs
                ) for f in funcs
            )
        )
    elif mode == 'trio':
        async def _trio_batch():
            return [
                await call_any(
                    f, *args, timeout=timeout, **kwargs
                ) for f in funcs
            ]
        return _trio_batch()
    else:
        return [
            call_any(f, *args, timeout=timeout, executor=executor, **kwargs)
            for f in funcs
        ]