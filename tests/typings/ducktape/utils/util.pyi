from typing import Any, Callable

from ducktape.errors import TimeoutError as TimeoutError

def wait_until(
    condition: Callable[[], Any],
    timeout_sec: float,
    backoff_sec: float = ...,
    err_msg: str | Callable[[], str] = ...,
    retry_on_exc: bool = ...,
) -> None:
    """Block until condition evaluates as true or timeout expires, whichever comes first.

    :param condition: callable that returns True if the condition is met, False otherwise
    :param timeout_sec: number of seconds to check the condition for before failing
    :param backoff_sec: number of seconds to back off between each failure to meet the condition before checking again
    :param err_msg: a string or callable returning a string that will be included as the exception message if the
                    condition is not met
    :param retry_on_exc: if True, will retry if condition raises an exception. If condition raised exception on last
                         iteration, that exception will be raised as a cause of TimeoutError.
                         If False and condition raises an exception, that exception will be forwarded to the caller
                         immediately.
                         Defaults to False (original ducktape behavior).
                         # TODO: [1.0.0] flip this to True
    :returns: silently if condition becomes true within the timeout window, otherwise raise Exception with the given error message.
    """
    ...

def package_is_installed(package_name): ...
def ducktape_version(): ...
def load_function(func_module_path): ...
