import asyncio
import logging

import textwrap

from synchronaut.utils import get_preferred_loop, is_uvloop, has_uvloop_policy

# ─── If uvloop is installed, make it the default asyncio event loop ───
if not has_uvloop_policy():
    logging.warning(textwrap.dedent('''
        ⚡️ Synchronaut: performance tip — installing uvloop for faster async scheduling.
        If you prefer your own event loop, set your policy before importing synchronaut.
    '''))

    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass

from synchronaut.synchronaut import synchronaut
from synchronaut.core import call_any, call_map, parallel_map, CallAnyTimeout

__all__ = [
    'synchronaut', 'call_any', 
    'call_map', 
    'CallAnyTimeout', 
    'parallel_map', 
    'get_preferred_loop', 
    'is_uvloop', 
    'has_uvloop_policy'
]

def main() -> None:
    print('Hello from synchronaut!')