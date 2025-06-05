## `synchronaut` Overview

**`synchronaut`** is a tiny bridge to write your business logic once and run it in both sync and async contexts—thread-safe, decorator-driven, and DB-friendly. It provides:

* A single `call_any` entrypoint for all sync↔️async combinations, where you can optionally pass `executor=`
* 🆕 A decorator `@synchronaut(...)` with `.sync` / `.async_` bypass methods
* Batch helper `parallel_map` (aliased as `parallel_map`), with per-call timeouts and exception capture
* Context-var propagation across threads
* Customizable timeouts with `CallAnyTimeout`

[![Package Version](https://img.shields.io/pypi/v/synchronaut.svg)](https://pypi.org/project/synchronaut/) | [![Supported Python Versions](https://img.shields.io/badge/Python->=3.10-blue?logo=python\&logoColor=white)](https://pypi.org/project/synchronaut/) | [![Pepy Total Downloads](https://img.shields.io/pepy/dt/synchronaut?color=2563EB\&cacheSeconds=3600)](https://pepy.tech/projects/synchronaut) | ![License](https://img.shields.io/github/license/cachetronaut/synchronaut) | ![GitHub Last Commit](https://img.shields.io/github/last-commit/cachetronaut/synchronaut)  | ![Status](https://img.shields.io/pypi/status/synchronaut) | [![Dynamic TOML Badge](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fcachetronaut%2Fsynchronaut%2Frefs%2Fheads%2Fmain%2Fpyproject.toml\&query=project.version\&prefix=v\&style=flat\&logo=github\&logoColor=1F51FF\&label=synchronaut\&labelColor=silver\&color=1F51FF)](https://github.com/cachetronaut/synchronaut)

## Quickstart

Install:

```bash
# “standard” install (no uvloop):
pip install synchronaut

# “fast” (with uvloop) for maximum asyncio performance:
pip install synchronaut[fast]
```

Create `quickstart.py`:

```python
import time
import asyncio

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
    print('call_any(sync_add):', await call_any(sync_add, 3, 4))

    # sync → async (in async context, sync funcs auto-offload)
    print('offloaded sync_add:', await call_any(sync_add, 5, 6))

    # async → async
    print('async_add:', await async_add(7, 8))
    print('call_any(async_add):', await call_any(async_add, 7, 8))

    # batch helper in async
    print('call_map:', await call_map([sync_add, async_add], 4, 5))

    # decorator shortcuts in async
    print('await dec_sync_add.async_:', await dec_sync_add.async_(6, 7))
    print('await dec_async_add:', await dec_async_add(8, 9))

    # timeout demo (pure-sync offload)
    try:
        await call_any(lambda: time.sleep(2), timeout=0.5)
    except CallAnyTimeout as e:
        print('Timeout caught:', e)

if __name__ == '__main__':
    # sync-land examples
    print('dec_sync_add(2,3):', dec_sync_add(2, 3))
    print('call_any(async_add) in sync:', call_any(async_add, 9, 10))
    # then run the async demonstrations
    asyncio.run(main())
```

Run it:

```bash
python quickstart.py
```

Expected output:

```bash
dec_sync_add(2,3): 5
sync_add: 3
call_any(sync_add): 7
offloaded sync_add: 11
async_add: 15
call_any(async_add): 15
call_map: [9, 9]
await dec_sync_add.async_: 13
await dec_async_add: 17
Timeout caught: Function <lambda> timed out after 0.5s
```

## FastAPI Integration

Copy this into `app.py`—it’ll just work once you `pip install synchronaut`:

```python
from typing import AsyncGenerator

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel

from synchronaut import synchronaut

# ——— Dummy DB & models ———
class User(BaseModel):
    id: int
    name: str

class DummyDB:
    def __init__(self):
        self._data = {
            1: {'id': 1, 'name': 'Alice'},
            2: {'id': 2, 'name': 'Bob'},
        }
    def query(self, user_id: int):
        return self._data.get(user_id)

async def get_db_async() -> AsyncGenerator[DummyDB, None]:
    db = DummyDB()
    try:
        yield db
    finally:
        ...

# ——— App & routes ———
app = FastAPI()

@synchronaut()
def get_user(user_id: int, db: DummyDB = Depends(get_db_async)) -> User:
    data = db.query(user_id)
    if not data:
        raise HTTPException(status_code=404, detail='User not found')
    return User(**data)

@app.get('/')
async def hello():
    return {"Hello, @syncronauts!"}

@app.get('/users/{user_id}', response_model=User)
async def read_user(user: User = Depends(get_user)):
    return user
```

Run:

```bash
uvicorn app:app --reload
```

This will produce:

```text
When you go to http://127.0.0.1:8000/ -> {'Hello, @syncronauts!'}
When you go to http://127.0.0.1:8000/users/1 -> {'id': 1, 'name': 'Alice'}
When you go to http://127.0.0.1:8000/users/2 -> {'id': 2, 'name': 'Bob'}
When you go to http://127.0.0.1:8000/users/3 -> {"detail":"User not found"}
```

> **Note:** if you ever need to offload into your own thread‐pool, you can write
>
> ```python
> call_any(some_sync_fn, arg1, arg2, executor=my_custom_executor)
> ```
>
> rather than relying on the built-in default.

## Context Propagation

Put this in `ctx_prop.py`:

```python
from synchronaut.utils import (
    request_context,
    spawn_thread_with_ctx,
    set_request_ctx,
    get_request_ctx,
)

# set a global context
set_request_ctx({'user_id': 42})
print('Global, user_id:', get_request_ctx()['user_id'])  # 42

# override in a block
with request_context({'user_id': 99}):
    print('Inside block, user_id:', get_request_ctx()['user_id'])  # 99

# back to global
print('Global again, user_id:', get_request_ctx()['user_id'])  # 42

# worker in a thread sees the global context
def work():
    print('Inside thread, user_id:', get_request_ctx()['user_id'])  # 42

thread = spawn_thread_with_ctx(work)
thread.join()
```

Run:

```bash
python ctx_prop.py
```

Expected:

```bash
Global, user_id: 42
Inside block, user_id: 99
Global again, user_id: 42
Inside thread, user_id: 42
```


## Batch Helper: `parallel_map`

A key feature in `synchronaut` is the ability to run multiple calls in parallel (in both sync and async contexts) with individual timeouts. This helper is exposed as `parallel_map` (aliased to `parallel_map` for backwards compatibility).

* In **sync-land**, all calls are submitted to a thread pool at once and run truly in parallel (up to `max_workers`).
* In **`asyncio`-land**, each call is wrapped in an `asyncio.create_task(...)` and then awaited with a single `asyncio.gather(...)`.
* In **Trio-land**, calls are run sequentially under Trio’s task runner—but any sync call still offloads to threads if needed.

### Signature

```python
def parallel_map(
    calls: list[tuple[Callable, tuple, dict, float|None]],
    *,
    executor: ThreadPoolExecutor | None = None,
    return_exceptions: bool = False,
) -> Any
```

* `calls` is a list of 4-tuples:

  ```python
  (func, args_tuple, kwargs_dict, per_call_timeout)
  ```

  where `per_call_timeout` is a `float` (seconds) or `None` (no timeout).
* `executor` (optional) lets you supply your own `ThreadPoolExecutor` for offloading in sync-land or asyncio-land; if omitted, the built-in `_SHARED_EXECUTOR` is used.
* `return_exceptions` (`bool`) controls whether exceptions get returned in the results list (instead of immediately propagating).

### Per-Function Timeouts

Each call’s 4th element is either:

* A `float` (e.g. `0.2`), causing `call_any(..., timeout=0.2, …)` to be used, so that if the function runs longer, a `CallAnyTimeout` is returned or raised.
* `None`, meaning no timeout is applied on that call.

### Exception Capture

* If `return_exceptions=False` (the default), the first exception (or timeout) anywhere will immediately bubble up.
* If `return_exceptions=True`, each call is wrapped in a `try/except` that returns the exception object in that position instead of raising.

### Examples

#### 1. Synchronous (plain‐old `def`) usage

```python
def sync_greet(name):
    return f"Hello, {name}!"

def sync_add(a, b):
    return a + b

def sync_sleep_and_return(x):
    import time; time.sleep(x)
    return f"Slept {x}s"

# We want:
#  - sync_greet("Alice")           with no timeout
#  - sync_add(2, 3)                with a 1.0s timeout
#  - sync_sleep_and_return(2)      with a 1.0s timeout  (this one should “timeout”)

calls = [
    (sync_greet, ("Alice",), {}, None),
    (sync_add, (2, 3), {}, 1.0),
    (sync_sleep_and_return, (2,), {}, 1.0),
]

# Collect exceptions instead of letting the sleep call raise
results = parallel_map(calls, return_exceptions=True)

# `results` is a list, in the same order:
# [
#   "Hello, Alice!",    # from sync_greet
#   5,                  # from sync_add
#   CallAnyTimeout(...) # from sync_sleep_and_return because it slept 2s > 1.0s timeout
# ]
print(results)
```

**Possible Output:**

```
['Hello, Alice!', 5, CallAnyTimeout('Function sync_sleep_and_return timed out after 1.0s')]
```

#### 2. “Mixed” Sync + Async in an `asyncio` coroutine

```python
import asyncio

async def async_hello(name):
    await asyncio.sleep(0.1)
    return f"Hi, {name}!"

def sync_double(x):
    return x * 2

async def main():
    calls = [
        # run async_hello("Charlie") with a 0.5s timeout
        (async_hello, ("Charlie",), {}, 0.5),

        # run sync_double(21) with no timeout
        (sync_double, (21,), {}, None),

        # run async_hello but force it to “time out” in 0.01s
        (async_hello, ("TooSlow",), {}, 0.01),
    ]

    # We want exceptions captured so we can see who timed out
    results = await parallel_map(calls, return_exceptions=True)

    # results[0] → "Hi, Charlie!"
    # results[1] → 42
    # results[2] → CallAnyTimeout(...) because async_hello slept 0.1s > 0.01s
    print(results)

asyncio.run(main())
```

**Possible Output:**

```
['Hi, Charlie!', 42, CallAnyTimeout('Function async_hello timed out after 0.01s')]
```

#### 3. Inside a Trio‐based function

```python
import trio

async def async_square(n):
    await trio.sleep(0.05)
    return n * n

def sync_subtract(a, b):
    return a - b

async def trio_main():
    calls = [
        (async_square, (5,),   {}, 0.1),   # finishes in 0.05s < 0.1s
        (sync_subtract,  (10,3),{}, None),
        (async_square, (10,),  {}, 0.01),  # will timeout
    ]

    results = await parallel_map(calls, return_exceptions=True)
    # → [25, 7, CallAnyTimeout(...)]
    print(results)

trio.run(trio_main)
```

**Possible Output:**

```
[25, 7, CallAnyTimeout('Function async_square timed out after 0.01s')]
```

## ⚙️ How It Works

1. **Signature**

   ```python
   def parallel_map(
       calls: list[tuple[Callable, tuple, dict, float|None]],
       *,
       executor: ThreadPoolExecutor | None = None,
       return_exceptions: bool = False,
   ) -> Any:
   ```

   * `calls` is a list of 4‐tuples:

     ```
     (func, args_tuple, kwargs_dict, per_call_timeout)
     ```

     where `per_call_timeout` is a `float` or `None`.
   * `executor` (optional) lets you supply your own `ThreadPoolExecutor` for offloading in sync-land or asyncio-land.
   * `return_exceptions` controls whether exceptions get returned in the result list (instead of immediately propagating).

2. **Sync branch (plain “no async loop”)**

   ```python
   futures = [
       executor.submit(
           partial(
               call_any,
               fn, *args,
               timeout=per_timeout,
               executor=executor,
               **kwargs
           )
       )
       for (fn, args, kwargs, per_timeout) in calls
   ]
   for fut in futures:
       try:
           results.append(fut.result())
       except Exception as exc:
           if return_exceptions:
               results.append(exc)
           else:
               raise
   ```

   * Each `call_any(...)` may, in turn, spin up an `asyncio.run_coroutine_threadsafe(...)` (for async fns) or run your sync fn directly in that thread.
   * Because all calls are submitted at once, they run in parallel up to `max_workers` threads in your pool.

3. **Asyncio branch (`mode == 'asyncio'`)**

   ```python
   async def _run_all_asyncio():
       tasks = []
       for (fn, args, kwargs, per_timeout) in calls:
           async def _run_one(fn=fn, args=args, kwargs=kwargs, per_timeout=per_timeout):
               try:
                   return await call_any(fn, *args, timeout=per_timeout, executor=_exec, **kwargs)
               except Exception as exc:
                   if return_exceptions:
                       return exc
                   raise
           tasks.append(asyncio.create_task(_run_one()))
       return await asyncio.gather(*tasks, return_exceptions=return_exceptions)
   ```

   * Each call is scheduled immediately with `asyncio.create_task(...)`.
   * Then a single `await asyncio.gather(...)` waits for all to complete or timeout/raise.

4. **Trio branch (`mode == 'trio'`)**

   ```python
   async def _run_all_trio():
       results = []
       for (fn, args, kwargs, per_timeout) in calls:
           try:
               val = await call_any(fn, *args, timeout=per_timeout, **kwargs)
               results.append(val)
           except Exception as exc:
               if return_exceptions:
                   results.append(exc)
               else:
                   raise
       return results
   ```

   * Calls are run sequentially under Trio’s task runner.
   * If any individual call raises, it’s either captured (if `return_exceptions=True`) or re‐raised.

## Advanced

All these options are callable via `call_any(...)` or the `@synchronaut(...)` decorator:

* **`timeout=`**: raises `CallAnyTimeout` if the call exceeds N seconds
* **`force_offload=True`**: always run sync funcs in the background loop (enables timely cancellation)
* **`executor=`**: send offloaded sync work into a caller-provided `ThreadPoolExecutor` (instead of the default)
* **`call_map([...], *args)`**: runs in parallel in async context, sequentially in sync context
* **Context propagation**:

  * `set_request_ctx()` / `get_request_ctx()` to set and read a global `ContextVar`
  * `request_context({...})` context-manager to temporarily override
  * `spawn_thread_with_ctx(fn, *args)` to ensure `ContextVar` state flows into threads

## ⚠️ Gotchas

1. **Decorator overhead**: each call does an inspect/async-check (nanoseconds–µs). In ultra-hot loops, consider a bypass.
2. **Timeouts on sync code**: pure-sync calls only respect `timeout` if offloaded—otherwise they block until completion.
3. **Background loop lifecycle**: offloads and `.sync` bypass use our single background loop; it lives until process exit.
4. **Custom executor**: if you pass `executor=my_executor`, that executor will actually be used for offloading. If you forget, all work goes into the built-in `_SHARED_EXECUTOR`.
5. **ContextVar propagation**: manual threads must use our `spawn_thread_with_ctx`.
6. **Non-asyncio stacks**: `_in_async_context` recognizes only asyncio and Trio. Other event loops may mis‐route.
7. **Tracebacks**: decorators + offloads can obscure original frames. Use logging or `inspect.trace()` for debugging.

## ✅ When **to** use synchronaut

1. **I/O-bound web services** (DB calls, HTTP, file I/O)
2. **Mixed sync/async code-bases** (one API, two contexts)
3. **FastAPI / DI**: sync ORMs auto-offload under the hood
4. **Context-scoped resources**: single “request context” across threads & coros

## 🚫 When **not** to use synchronaut

1. **CPU-bound tight loops** where microseconds matter
2. **Pure-sync or pure-async projects** (no context switching)
3. **Non-asyncio async frameworks** (e.g. Curio)
4. **Strict loop-lifecycle environments** that forbid background loops

> By tuning `timeout`, `force_offload`, or using the `.sync`/`.async_` bypasses, you get seamless sync↔️async interoperability without rewriting your core logic.