"""
Improved deferred library for App Engine

some description here
"""
import time

from google.appengine.api import taskqueue
from google.appengine.runtime.apiproxy_errors import DeadlineExceededError
from google.appengine.ext import deferred as gae_deferred


# How many times try to put task onto queue before giving up?
_MAX_RETRIES = 5


def _execute(executor, *args, **kwargs):
    times_tried = 0
    while True:
        try:
            executor(*args, **kwargs)
        except DeadlineExceededError:
            if times_tried >= _MAX_RETRIES:
                raise
            time.sleep(1)  # I know what I'm doing, trust me
            continue
        except taskqueue.TaskAlreadyExistsError:
            pass
        except taskqueue.TombstonedTaskError:
            pass
        break


def defer(*args, **kwargs):
    _execute(
        gae_deferred.defer,
        *args,
        **kwargs
    )
