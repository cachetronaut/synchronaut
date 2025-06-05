from synchronaut.synchronaut import synchronaut
from synchronaut.core import call_any, call_map, parallel_map, CallAnyTimeout

__all__ = ['synchronaut', 'call_any', 'call_map', 'CallAnyTimeout', 'parallel_map']

def main() -> None:
    print('Hello from synchronaut!')
