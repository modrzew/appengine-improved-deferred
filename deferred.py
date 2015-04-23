"""
Improved deferred library for App Engine

some description here
"""
import hashlib
import logging
import pickle
import time

from google.appengine.api import taskqueue
from google.appengine.runtime.apiproxy_errors import DeadlineExceededError
from google.appengine.ext import deferred as gae_deferred


# How many times try to put task onto queue before giving up?
MAX_RETRIES = 5
# Where to put the task, if taskgiver doesn't specify queue/module?
DEFAULT_MODULE = None
DEFAULT_QUEUE = None


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
            times_tried += 1
            if times_tried >= MAX_RETRIES:
                raise
            time.sleep(0.5 * times_tried)  # I know what I'm doing, trust me
            continue
        except taskqueue.TaskAlreadyExistsError:
            logging.warning('Task already exists!')
        except taskqueue.TombstonedTaskError:
            logging.warning('Task tombstoned!')
        break


def _generate_hash(args, kwargs):
    """Generates hash for given args/kwargs"""
    return hashlib.sha256(pickle.dumps((args, kwargs))).hexdigest()


def deferred(identifier):
    """Decorator for deferred functions

    If function that is decorated with this is deferred with our own defer,
    it will be enqueued using taskqueue.add and our own handler.

    `identifier` will be appended to URL, allowing you to identify that task
    in the logs.
    """
    def inner(func):
        func.__deferred_identifier = identifier
        return func
    return inner


def defer(func_or_path, *args, **kwargs):
    """Intelligent task deferrer

    You can use it as you would use deferred.defer - just pass it a function,
    some kwargs and you're all set!
    If function is decorated with @deferred_task, it shall be delegated using
    taskqueue.add and DeferredHandler instead of deferred.defer.
    """
    # If taskgiver didn't specify a name, try to guess one
    if '_name' not in kwargs:
        kwargs['_name'] = '{func_name}-{task_hash}'.format(
            func_name='todo',
            task_hash=_generate_hash(args, kwargs),
        )
    # TODO: make use of taskqueue.add
    _execute(
        gae_deferred.defer,
        func_or_path,
        *args,
        **kwargs
    )
