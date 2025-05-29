from synchronaut.utils import (
    request_context, 
    spawn_thread_with_ctx, 
    set_request_ctx, 
    get_request_ctx
)

def test_context_manager_and_thread_propagation():
    set_request_ctx({'user_id': 42})
    assert get_request_ctx()['user_id'] == 42

    with request_context({'user_id': 99}):
        assert get_request_ctx()['user_id'] == 99

    # back to global
    assert get_request_ctx()['user_id'] == 42

    def work():
        assert get_request_ctx()['user_id'] == 42

    thread = spawn_thread_with_ctx(work)
    thread.join()

# set a global context
set_request_ctx({'user_id': 42})

def work():
    print('Inside thread, user_id:', get_request_ctx()['user_id'])

# context manager
with request_context({'user_id': 99}):
    print('Inside block, user_id:', get_request_ctx()['user_id'])

# back to global
print('Global, user_id:', get_request_ctx()['user_id'])

# spawn a thread that carries the current global context
thread = spawn_thread_with_ctx(work)
thread.join()

print('ctx_prop.py: all assertions passed')