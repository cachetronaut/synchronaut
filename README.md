## `synchronaut` Overview
**`synchronaut`** is a tiny bridge to write your business logic once and run it in both sync and async contexts—thread-safe, decorator-driven, and DB-friendly. It provides:
- A single `call_any` entrypoint for all sync↔️async combinations, where you can optionally pass `executor=`
- A decorator `@synchronaut(...)` with `.sync` / `.async_` bypass methods
- Batch helper `call_map`
- Context-var propagation across threads
- Customizable timeouts with `CallAnyTimeout`

[![Package Version](https://img.shields.io/pypi/v/synchronaut.svg)](https://pypi.org/project/synchronaut/) | [![Supported Python Versions](https://img.shields.io/badge/Python->=3.10-blue?logo=python&logoColor=white)](https://pypi.org/project/synchronaut/) | [![Pepy Total Downloads](https://img.shields.io/pepy/dt/synchronaut?color=2563EB&cacheSeconds=3600)](https://pepy.tech/projects/synchronaut) | ![License](https://img.shields.io/github/license/cachetronaut/synchronaut) | ![GitHub Last Commit](https://img.shields.io/github/last-commit/cachetronaut/synchronaut)  | ![Status](https://img.shields.io/pypi/status/synchronaut) | [![Dynamic TOML Badge](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fcachetronaut%2Fsynchronaut%2Frefs%2Fheads%2Fmain%2Fpyproject.toml&query=project.version&prefix=v&style=flat&logo=github&logoColor=1F51FF&label=synchronaut&labelColor=silver&color=1F51FF)](https://github.com/cachetronaut/synchronaut)

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
> ```python
> call_any(some_sync_fn, arg1, arg2, executor=my_custom_executor)
> ```
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
## Advanced
All these options are callable via `call_any(...)` or the `@synchronaut(...)` decorator:
- **`timeout=`**: raises `CallAnyTimeout` if the call exceeds N seconds
- **`force_offload=True`**: always run sync funcs in the background loop (enables timely cancellation)
- **`executor=`**: send offloaded sync work into a caller-provided `ThreadPoolExecutor` (instead of the default)
- **`call_map([...], *args)`**: runs in parallel in async context, sequentially in sync context
- **Context propagation**:
    - `set_request_ctx()` / `get_request_ctx()` to set and read a global `ContextVar`
    - `request_context({...})` context-manager to temporarily override
    - `spawn_thread_with_ctx(fn, *args)` to ensure `ContextVar` state flows into threads
## ⚠️ Gotchas
1. **Decorator overhead**: each call does an inspect/async-check (nanoseconds–µs). In ultra-hot loops, consider a bypass.
2. **Timeouts on sync code**: pure-sync calls only respect `timeout` if offloaded—otherwise they block until completion.
3. **Background loop lifecycle**: offloads and `.sync` bypass use our single background loop; it lives until process exit.
4. **Custom executor**: if you pass `executor=my_executor`, that executor will actually be used for offloading. If you forget, all work goes into the built-in `_SHARED_EXECUTOR`.
5. **ContextVar propagation**: manual threads must use our `spawn_thread_with_ctx`.
6. **Non-asyncio stacks**: `_in_async_context` recognizes only asyncio and Trio. Other event loops may mis-route.
7. **Tracebacks**: decorators + offloads can obscure original frames. Use logging or `inspect.trace()` for debugging.
## ✅ When **to** use synchronaut
- **I/O-bound web services** (DB calls, HTTP, file I/O)
- **Mixed sync/async code-bases** (one API, two contexts)
- **FastAPI / DI**: sync ORMs auto-offload under the hood
- **Context-scoped resources**: single “request context” across threads & coros
## 🚫 When **not** to use synchronaut
1. **CPU-bound tight loops** where microseconds matter
2. **Pure-sync or pure-async projects** (no context switching)
3. **Non-asyncio async frameworks** (e.g. Curio)
4. **Strict loop-lifecycle environments** that forbid background loops

> By tuning `timeout`, `force_offload`, or using the `.sync`/`.async_` bypasses, you get seamless sync↔️async interoperability without rewriting your core logic.