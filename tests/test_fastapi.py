from typing import AsyncGenerator

from fastapi import FastAPI, Depends, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel

from synchronaut import synchronaut

# ——— Setup ———
# ——— Dummy DB & models ———
class User(BaseModel):
    id: int
    name: str

class DummyDB:
    def __init__(self):
        # pretend this is your ORM/data source
        self._data = {
            1: {'id': 1, 'name': 'Alice'},
            2: {'id': 2, 'name': 'Bob'},
        }

    def query(self, user_id: int):
        # sync API: like Session.query(User).get(user_id)
        return self._data.get(user_id)

async def get_db_async() -> AsyncGenerator[DummyDB, None]:
    '''
    FastAPI async dependency that yields a DB session.
    '''
    db = DummyDB()
    try:
        yield db
    finally:
        # clean up if needed
        ...

# ——— App & routes ———
app = FastAPI()

@synchronaut()
def get_user(user_id: int, db: DummyDB = Depends(get_db_async)) -> User:
    '''
    Sync function that FastAPI will call in both sync and async contexts.
    '''
    data = db.query(user_id)
    if not data:
        raise HTTPException(status_code=404, detail='User not found')
    return User(**data)

@app.get('/users/{user_id}', response_model=User)
async def read_user(user: User = Depends(get_user)):
    return user

# ——— Test ———
client = TestClient(app)

def test_read_user_success():
    resp = client.get('/users/1')
    assert resp.status_code == 200
    print(f'{resp.status_code == 200 = }')
    print(f'{resp.status_code = }')
    assert resp.json() == {'id': 1, 'name': 'Alice'}
    print(f'{resp.json() == {'id': 1, 'name': 'Alice'} = }')
    print(f'{resp.json() = }')

def test_read_user_not_found():
    resp = client.get('/users/999')
    assert resp.status_code == 404
    print(f'{resp.status_code == 404 = }')
    print(f'{resp.status_code = }')
    assert resp.json() == {'detail': 'User not found'}
    print(f'{resp.json() == {'detail': 'User not found'} = }')
    print(f'{resp.json() = }')

if __name__ == '__main__':
    test_read_user_success()
    test_read_user_not_found()