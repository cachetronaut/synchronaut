import inspect
import asyncio
from functools import wraps
from synchronaut.core import call_any, _start_background_loop

def synchronaut(
    *,
    timeout: float = None,
    executor = None,
    force_offload: bool = False,
    reuse_loop: bool = False
):
    '''
    Decorator factory exposing the same options as call_any.
    '''
    def _decorate(func):
        sig = inspect.signature(func)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return call_any(
                func, *args,
                timeout=timeout,
                executor=executor,
                force_offload=force_offload,
                reuse_loop=reuse_loop,
                **kwargs
            )

        if inspect.iscoroutinefunction(func):
            # .sync will drive the coroutine to completion on our background loop,
            # so it works in any context (even inside a running loop).
            def run_sync(*args, **kwargs):
                loop = _start_background_loop()
                future = asyncio.run_coroutine_threadsafe(
                    func(*args, **kwargs),
                    loop
                )
                return future.result() # no timeout here—honor timeout via call_any if needed

            sync_wrapper.sync = run_sync
            sync_wrapper.async_ = func  # original coroutine

        else:
            # .async_ will offload the sync function in async-land
            async def run_async(*args, **kwargs):
                return await call_any(
                    func, *args,
                    timeout=timeout,
                    executor=executor,
                    force_offload=True,
                    reuse_loop=reuse_loop,
                    **kwargs
                )
            sync_wrapper.sync = func # original sync func
            sync_wrapper.async_ = run_async

        # Preserve original signature for introspection tools
        sync_wrapper.__signature__ = sig
        return sync_wrapper

    return _decorate
