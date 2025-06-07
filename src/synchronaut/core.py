import asyncio
import inspect
import threading

from functools import partial, lru_cache
from typing import Any, Callable
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
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
_SHARED_EXECUTOR = ThreadPoolExecutor(max_workers=8)  # you can bump max_workers as needed

class CallAnyTimeout(Exception):
    '''Raised when call_any(...) exceeds the given timeout.'''
    pass

@lru_cache(maxsize=None)
def _is_coro_fn(fn: Callable) -> bool:
    '''Returns True if `fn` is declared as `async def`.'''
    return inspect.iscoroutinefunction(fn)

def _in_async_context() -> str | None:
    '''
    Detect if we are currently inside an asyncio loop or a Trio loop.
    Returns:
      - 'asyncio' if asyncio.get_running_loop() succeeds,
      - 'trio' if trio.lowlevel.current_trio_token() is non‐None,
      - None if neither.
    '''
    try:
        asyncio.get_running_loop()
        return 'asyncio'
    except RuntimeError:
        pass

    try:
        token = trio.lowlevel.current_trio_token()
        return 'trio' if (token is not None) else None
    except Exception:
        return None

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
    - “Async-land'
        - If called from inside an asyncio or Trio loop:
            - If `func` is `async def`: returns a coroutine that the caller must `await`.
            - If `func` is `def`, offloads it into a thread (via `anyio.to_thread.run_sync`) 
            and returns a coroutine to await.
        - In both cases, if `timeout` is set, we wrap with a failsafe so that exceeding 
        the timeout raises CallAnyTimeout.
    - “Sync-land'
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

    # ─── Async‐land ───
    if mode == 'asyncio':
        loop = asyncio.get_running_loop()

        if is_coro:
            # async def under asyncio
            if timeout is not None:
                async def _aio_with_timeout():
                    try:
                        return await asyncio.wait_for(func(*args, **kwargs), timeout)
                    except asyncio.TimeoutError as e:
                        raise CallAnyTimeout(
                            f'Function {func.__name__} timed out after {timeout}s'
                        ) from e
                return _aio_with_timeout()
            return func(*args, **kwargs)

        # plain‐sync def under asyncio
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
            return loop.run_in_executor(
                target_exec, partial(func, *args, **kwargs)
            )

        async def _aio_direct():
            return loop.run_in_executor(
                target_exec, partial(func, *args, **kwargs)
            )

        return _aio_direct()

    elif mode == 'trio':
        # inside a Trio run loop
        if is_coro:
            if timeout is not None:
                async def _trio_with_timeout():
                    try:
                        with anyio.fail_after(timeout):
                            return await func(*args, **kwargs)
                    except Exception:
                        raise CallAnyTimeout(
                            f'Function {func.__name__} timed out after {timeout}s'
                        )
                return _trio_with_timeout()
            return func(*args, **kwargs)

        # plain‐sync def under Trio
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

        async def _trio_direct():
            return func(*args, **kwargs)
        return _trio_direct()

    # ─── Sync‐land ───
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

    if force_offload or (timeout is not None):
        if executor is not None:
            future2 = executor.submit(func, *args, **kwargs)
            try:
                return future2.result(timeout)
            except FutureTimeoutError as e:
                raise CallAnyTimeout(
                    f'Function {func.__name__} timed out after {timeout}s'
                ) from e

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

    # ─── Bare‐metal Sync call ───
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

# ─── New: supafast version of call_map ───
def parallel_map(
    calls: list[tuple[Callable, tuple, dict, float | None]],
    *,
    executor: ThreadPoolExecutor | None = None,
    return_exceptions: bool = False,
) -> Any:
    '''
    Enhanced version of call_map that accepts per‐function timeouts and optional exception capturing.

    `calls` should be a list of 4‐tuples:
        (func, args_tuple, kwargs_dict, per_call_timeout_or_None)

    - If in asyncio: schedules all calls concurrently via asyncio.gather.
    - If in Trio: schedules all calls concurrently (awaited in a comprehension).
    - If in plain sync: offloads each `call_any(...)` to threads, gathering results in parallel.

    Parameters:
    - calls: List of (fn, args, kwargs, timeout)
    - executor: Optional ThreadPoolExecutor to use (defaults to module’s shared pool).
    - return_exceptions: If True, collects exceptions in the result list instead of raising.

    Returns:
      - In asyncio: an awaitable (you must `await`) that yields a list of results/exceptions.
      - In Trio: a coroutine that you `await` to get a list of results/exceptions.
      - In Sync: a plain Python list of results/exceptions.
    '''
    mode = _in_async_context()
    _exec = executor or _SHARED_EXECUTOR

    # ─── Asyncio Mode ───
    if mode == 'asyncio':
        async def _run_all_asyncio():
            tasks = []
            for fn, args, kwargs, per_timeout in calls:
                # wrap each call_any(...) so that exceptions can be caught if requested
                async def _run_one(inner_fn=fn, inner_args=args, inner_kwargs=kwargs, inner_to=per_timeout):
                    try:
                        return await call_any(
                            inner_fn, *inner_args,
                            timeout=inner_to,
                            executor=_exec,
                            **inner_kwargs
                        )
                    except Exception as exc:
                        if return_exceptions:
                            return exc
                        raise

                # schedule it immediately
                tasks.append(asyncio.create_task(_run_one()))

            # use gather with/without return_exceptions per flag
            if return_exceptions:
                return await asyncio.gather(*tasks, return_exceptions=True)
            else:
                return await asyncio.gather(*tasks)

        return _run_all_asyncio()

    # ─── Trio Mode ───
    elif mode == 'trio':
        async def _run_all_trio():
            results = []
            for fn, args, kwargs, per_timeout in calls:
                try:
                    val = await call_any(
                        fn, *args,
                        timeout=per_timeout,
                        **kwargs
                    )
                    results.append(val)
                except Exception as exc:
                    if return_exceptions:
                        results.append(exc)
                    else:
                        raise
            return results

        return _run_all_trio()

    # ─── Plain Sync Mode ───
    else:
        futures = []
        for fn, args, kwargs, per_timeout in calls:
            # offload each call_any(...) into a thread so all run concurrently
            futures.append(
                _exec.submit(
                    partial(
                        call_any,
                        fn, *args,
                        timeout=per_timeout,
                        executor=_exec,
                        **kwargs
                    )
                )
            )

        results = []
        for fut in futures:
            try:
                results.append(fut.result())  # per-call timeout is enforced inside call_any
            except Exception as exc:
                if return_exceptions:
                    results.append(exc)
                else:
                    raise
        return results