"""
Improved deferred library for App Engine

some description here
"""
import logging
import time

from google.appengine.api import taskqueue
from google.appengine.runtime.apiproxy_errors import DeadlineExceededError
from google.appengine.ext import deferred as gae_deferred


# How many times try to put task onto queue before giving up?
_MAX_RETRIES = 5


def _execute(executor, *args, **kwargs):
    """Puts task function on the queue using provided executor

    Executor may be either deferred.defer or taskqueue.add.
    In case of DeadlineExceededError, it does its best to retry deferring the
    task. And taskqueue can be randomly unavailable for short spans, or RPC
    can return after too long time (and sadly, there's no way to change its
    deadline).
    It also catches two exceptions that may be raised by taskqueue:
    - TaskAlreadyExistsError (in case task is alreaady present on the queue),
    - TombstonedTaskError (in case task was already executed).
    """
    times_tried = 0
    while True:
        try:
            executor(*args, **kwargs)
        except DeadlineExceededError:
            if times_tried >= _MAX_RETRIES:
                raise
            time.sleep(0.5 * times_tried)  # I know what I'm doing, trust me
            continue
        except taskqueue.TaskAlreadyExistsError:
            logging.warning('Task already exists!')
        except taskqueue.TombstonedTaskError:
            logging.warning('Task tombstoned!')
        break


def _generate_name(args, kwargs):
    """Generates unique name for given args/kwargs"""
    return 'unique-name'  # will suffice for now


def defer(func_or_path, *args, **kwargs):
    """Defers the task in an intelligent way"""
    if '_name' not in kwargs:
        kwargs['_name'] = _generate_name(args, kwargs)
    _execute(
        gae_deferred.defer,
        func_or_path,
        *args,
        **kwargs
    )
