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
import webapp2


# How many times try to put task onto queue before giving up?
MAX_RETRIES = 5
# Where to put the task, if taskgiver doesn't specify queue/module?
DEFAULT_MODULE = None
DEFAULT_QUEUE = None
# Deferred handler URL
DEFERRED_URL = '/deferred/'


def _execute(executor, *args, **kwargs):
    """Puts task function on the queue using provided executor

    Executor may be either deferred.defer or taskqueue.add.
    In case of DeadlineExceededError, it does its best to retry deferring the
    task. And taskqueue can be randomly unavailable for short spans, or RPC
    can return after too long time (and sadly, there's no way to change its
    deadline).
    It also catches two exceptions that may be raised by taskqueue:
    - TaskAlreadyExistsError (in case task is alreaady present on the queue),
    - TombstonedTaskError (in case task was already executed),
    - TaskTooLargeError (in case payload is too big).
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
        except taskqueue.TaskTooLargeError:
            logging.exception('Task is too large to execute!')
            logging.debug('Args: %s', args)
            logging.debug('Kwargs: %s', kwargs)
        break


def _generate_hash(args, kwargs):
    """Generates hash for given args/kwargs"""
    return hashlib.sha256(pickle.dumps((args, kwargs))).hexdigest()


def _prepare_taskqueue_kwargs(path, args, kwargs):
    """Converts deferred kwargs to taskqueue kwargs

    Returns tuple: (payload, taskqueue_kwargs)
    """
    payload = {
        'args': args,
        'kwargs': kwargs,
        'path': path,
    }
    taskqueue_kwargs = {}
    # Basically, all arguments beginning with underscore should be extracted
    for k in kwargs.keys():
        if k == '_queue':  # ...with this slight exception
            taskqueue_kwargs['queue_name'] = kwargs.pop(k)
        elif k.startswith('_'):
            taskqueue_kwargs[k] = kwargs.pop(k)
    return payload, taskqueue_kwargs


class InvalidPath(Exception):
    """Raised when there's something wrong with import path"""


def _load(path):
    """Loads function lazily based on the import path

    Supports loading:
    - standalone functions,
    - class/static methods.
    """
    def some_function(*args, **kwargs):
        pass
    return some_function


class DeferredHandler(webapp2.RequestHandler):
    """Handling deferred functions in a better way"""
    def post(self, identifier):
        unpickled = pickle.loads(self.request.body)
        args = unpickled.get('args', ())
        kwargs = unpickled.get('kwargs', {})
        path = unpickled.get('path')
        if not path:
            raise self.abort(400, 'No path')
        try:
            function = _load(path)
        except InvalidPath as e:
            raise self.abort(400, 'Invalid path: %s' % e.message)
        function(*args, **kwargs)
        self.response.status = 200


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


def defer(func, *args, **kwargs):
    """Intelligent task deferrer

    You can use it as you would use deferred.defer - just pass it a function,
    some kwargs and you're all set!
    If function is decorated with @deferred_task, it shall be delegated using
    taskqueue.add and DeferredHandler instead of deferred.defer.
    """
    identifier = func.__deferred_identifier
    # If taskgiver didn't specify a name, try to guess one
    if '_name' not in kwargs:
        kwargs['_name'] = '{func_name}-{task_hash}'.format(
            func_name='todo',
            task_hash=_generate_hash(args, kwargs),
        )
    # Also, take care of some default values
    if '_queue' not in kwargs:
        kwargs['_queue'] = DEFAULT_QUEUE
    if '_target' not in kwargs:
        kwargs['_target'] = DEFAULT_MODULE
    # Should we use taskqueue.add?
    if hasattr(func, '__deferred_identifier'):
        # In order to be able to import the function later
        path = '{module}.{function}'.format(
            module=func.__module__,
            function=func.__name__,
        )
        # taskqueue.add has slightly different interface than deferred.defer
        payload, taskqueue_kwargs = _prepare_taskqueue_kwargs(
            path,
            args,
            kwargs,
        )
        _execute(
            taskqueue.add,
            url=DEFERRED_URL % identifier,
            method='POST',
            payload=pickle.dumps(payload),
            **taskqueue_kwargs
        )
    # Or deferred.defer?
    else:
        _execute(
            gae_deferred.defer,
            func,
            *args,
            **kwargs
        )


# webapp2 route definition - so you can just import it from here
ROUTE = webapp2.Route(
    DEFERRED_URL % '<identifier:.+>',
    DeferredHandler,
)
