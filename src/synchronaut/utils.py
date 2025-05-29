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