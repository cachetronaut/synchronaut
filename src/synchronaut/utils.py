import asyncio
import queue
import contextvars
import threading

from contextlib import contextmanager
from typing import Any, Callable

# ContextVar for request/session context
_request_ctx: contextvars.ContextVar[Any] = contextvars.ContextVar('request_ctx')

def set_request_ctx(ctx: Any) -> None:
    _request_ctx.set(ctx)

def get_request_ctx(default=None) -> Any:
    return _request_ctx.get(default)

@contextmanager
def request_context(ctx: Any):
    '''
    Temporarily set a request context for a block.
    '''
    token = _request_ctx.set(ctx)
    try:
        yield
    finally:
        _request_ctx.reset(token)

def spawn_thread_with_ctx(target: Callable, *args, **kwargs) -> threading.Thread:
    '''
    Spawn a std-lib Thread that propagates the current ContextVar.
    '''
    ctx = get_request_ctx()
    q: queue.Queue[tuple[Any, BaseException | None]] = queue.Queue()

    def wrapped(*a, **k):
        set_request_ctx(ctx)
        try:
            res = target(*a, **k)
            q.put((res, None))
        except Exception as e:
            q.put((None, e))

    thread = threading.Thread(target=wrapped, args=args, kwargs=kwargs, daemon=True)
    thread.start()
    return thread

def get_preferred_loop() -> asyncio.AbstractEventLoop:
    '''
    1. If inside async code, return the running loop.
    2. Else, try asyncio.get_event_loop() (which under the hood uses the
       currently-installed policy, uvloop or default).
    3. If that fails, make a new loop, set it on this thread, and return it.
    '''
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        pass

    try:
        # Does not warn, and uses policy’s factory
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
    except Exception:
        # very unlikely to hit
        return asyncio.get_event_loop_policy().get_event_loop()

def is_uvloop(loop: asyncio.AbstractEventLoop | None = None) -> bool:
    '''
    Return True only if BOTH:
    1) The global policy is uvloop.EventLoopPolicy, and
    2) The loop (or preferred loop) is an instance of uvloop.Loop.
    '''
    try:
        import uvloop
    except ImportError:
        return False

    if not isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy):
        return False

    loop = loop or get_preferred_loop()
    return isinstance(loop, uvloop.Loop)

def has_uvloop_policy() -> bool:
    '''
    Return True if the global asyncio event-loop policy is
    uvloop.EventLoopPolicy (i.e. the user already did `uvloop.install()`).
    '''
    policy = asyncio.get_event_loop_policy()
    try:
        import uvloop
        return isinstance(policy, uvloop.EventLoopPolicy)
    except ImportError:
        return False
