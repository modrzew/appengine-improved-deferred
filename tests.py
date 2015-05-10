import hashlib
import pickle
import unittest

import mock
import pytest

from google.appengine.api import taskqueue
from google.appengine.runtime.apiproxy_errors import DeadlineExceededError

import deferred


class Parent(object):
    class Inner(object):
        @staticmethod
        def function():
            pass

    @classmethod
    def function(cls):
        pass


def top_level_function():
    pass


class LoadTests(unittest.TestCase):
    def test_top_level(self):
        func = deferred._load('tests.top_level_function')
        assert func is top_level_function

    def test_classmethod(self):
        func = deferred._load('tests.Parent.function')
        assert func.__code__.co_code == Parent.function.__code__.co_code

    def test_inner_classmethod(self):
        func = deferred._load('tests.Parent.Inner.function')
        assert func is Parent.Inner.function

    def test_no_module(self):
        with pytest.raises(deferred.InvalidPath):
            deferred._load('some_module')

    def test_no_module_nested(self):
        with pytest.raises(deferred.InvalidPath):
            deferred._load('some.module.function')

    def test_no_module_attribute(self):
        with pytest.raises(deferred.InvalidPath):
            deferred._load('tests.something_nonexistent')

    def test_no_class_attribute(self):
        with pytest.raises(deferred.InvalidPath):
            deferred._load('tests.Parent.nonexistent')

    def test_empty(self):
        with pytest.raises(deferred.InvalidPath):
            deferred._load('')


class GenerateHashTests(unittest.TestCase):
    def test_ok(self):
        args = (1, 2, 'some', 'thing')
        kwargs = {
            'another': 'thing',
            'value': 42,
        }
        expected = hashlib.md5(pickle.dumps((args, kwargs))).hexdigest()
        result = deferred._generate_hash(args, kwargs)
        assert result == expected


@mock.patch.object(deferred.time, 'sleep', mock.MagicMock())
class ExecuteTests(unittest.TestCase):
    def setUp(self):
        super(ExecuteTests, self).setUp()
        self.executor = mock.MagicMock()

    def test_ok(self):
        deferred._execute(self.executor, 1, 2, 'a', 'mama', some='thing')
        self.executor.assert_called_once_with(
            1,
            2,
            'a',
            'mama',
            some='thing',
        )

    @mock.patch.object(deferred, 'MAX_RETRIES', 5)
    def test_retry(self):
        self.executor.side_effect = [DeadlineExceededError] * 3 + [None]
        deferred._execute(self.executor, 1, 2, 'a', 'mama', some='thing')
        assert self.executor.call_count == 4

    @mock.patch.object(deferred, 'MAX_RETRIES', 5)
    def test_retry_raise(self):
        self.executor.side_effect = [DeadlineExceededError] * 6
        with pytest.raises(DeadlineExceededError):
            deferred._execute(self.executor, 1, 2, 'a', 'mama', some='thing')
        assert self.executor.call_count == 5

    @mock.patch.object(deferred, 'MAX_RETRIES', 5)
    @mock.patch.object(deferred, 'logging')
    def test_already_exists(self, m_l):
        error = taskqueue.TaskAlreadyExistsError
        self.executor.side_effect = [DeadlineExceededError] * 3 + [error]
        deferred._execute(self.executor, 1, 2, 'a', 'mama', some='thing')
        assert self.executor.call_count == 4
        assert m_l.warning.called

    @mock.patch.object(deferred, 'MAX_RETRIES', 5)
    @mock.patch.object(deferred, 'logging')
    def test_tombstoned(self, m_l):
        error = taskqueue.TombstonedTaskError
        self.executor.side_effect = [DeadlineExceededError] * 3 + [error]
        deferred._execute(self.executor, 1, 2, 'a', 'mama', some='thing')
        assert self.executor.call_count == 4
        assert m_l.warning.called

    @mock.patch.object(deferred, 'MAX_RETRIES', 5)
    @mock.patch.object(deferred, 'logging')
    def test_too_large(self, m_l):
        error = taskqueue.TaskTooLargeError
        self.executor.side_effect = [DeadlineExceededError] * 3 + [error]
        deferred._execute(self.executor, 1, 2, 'a', 'mama', some='thing')
        assert self.executor.call_count == 4
        assert m_l.exception.called


class PrepareTaskqueueKwargsTests(unittest.TestCase):
    def test_normal(self):
        payload, taskqueue_kwargs = deferred._prepare_taskqueue_kwargs(
            '/some/path',
            (1, 2, 'a', 'c'),
            {
                'some': 'thing',
                'other': 'thing',
                '3': 5,
            },
        )
        assert taskqueue_kwargs == {}
        assert payload == {
            'path': '/some/path',
            'args': (1, 2, 'a', 'c'),
            'kwargs': {
                'some': 'thing',
                'other': 'thing',
                '3': 5,
            },
        }

    def test_taskqueue_kwargs(self):
        payload, taskqueue_kwargs = deferred._prepare_taskqueue_kwargs(
            '/some/path',
            (1, 2, 'a', 'c'),
            {
                'some': 'thing',
                'other': 'thing',
                '3': 5,
                '_some_arg': 'something',
                '_queue': 'not-default',
                '_target': 'mymodule',
                '_countdown': 120,
            },
        )
        assert taskqueue_kwargs == {
            '_some_arg': 'something',
            'queue_name': 'not-default',
            '_target': 'mymodule',
            '_countdown': 120,
        }
        assert payload == {
            'path': '/some/path',
            'args': (1, 2, 'a', 'c'),
            'kwargs': {
                'some': 'thing',
                'other': 'thing',
                '3': 5,
            },
        }


class DecoratorTests(unittest.TestCase):
    def test_ok(self):
        @deferred.deferred('/some/path/')
        def function():
            return 123
        identifier = getattr(function, '__deferred_identifier')
        assert identifier == '/some/path/'
        assert function() == 123


class DeferTests(unittest.TestCase):
    def setUp(self):
        super(DeferTests, self).setUp()
        self.patches = (
            mock.patch.object(deferred.taskqueue, 'add'),
            mock.patch.object(deferred.gae_deferred, 'defer'),
            mock.patch.object(deferred, 'DEFAULT_MODULE', 'somemodule'),
            mock.patch.object(deferred, 'DEFAULT_QUEUE', 'somequeue'),
        )
        self.taskqueue, self.defer, _, _ = [p.start() for p in self.patches]

    def tearDown(self):
        super(DeferTests, self).tearDown()
        for patcher in self.patches:
            patcher.stop()

    def test_defer(self):
        deferred.defer(top_level_function)
        assert not self.taskqueue.called
        self.defer.assert_called_once_with(
            top_level_function,
            _name=mock.ANY,
            _queue='somequeue',
            _target='somemodule',
        )

    def test_non_default_queue(self):
        deferred.defer(top_level_function, _queue='differentqueue')
        assert not self.taskqueue.called
        self.defer.assert_called_once_with(
            top_level_function,
            _name=mock.ANY,
            _queue='differentqueue',
            _target='somemodule',
        )

    def test_non_default_target(self):
        deferred.defer(top_level_function, _target='differentmodule')
        assert not self.taskqueue.called
        self.defer.assert_called_once_with(
            top_level_function,
            _name=mock.ANY,
            _queue='somequeue',
            _target='differentmodule',
        )

    def test_name_given(self):
        deferred.defer(top_level_function, _name='taskname')
        assert not self.taskqueue.called
        self.defer.assert_called_once_with(
            top_level_function,
            _name='taskname',
            _queue='somequeue',
            _target='somemodule',
        )

    def test_name_random(self):
        deferred.defer(top_level_function)
        assert not self.taskqueue.called
        name = self.defer.call_args[1]['_name']
        assert name.startswith('top_level_function-')
        assert len(name) == (len('top_level_function-') + 32)

    @mock.patch.object(deferred, 'DEFERRED_URL', '/defer/%s')
    def test_taskqueue(self):
        decorated = deferred.deferred('some/path')(top_level_function)
        deferred.defer(decorated)
        assert not self.defer.called
        assert self.taskqueue.called
        kwargs = self.taskqueue.call_args[1]
        assert kwargs['url'] == '/defer/some/path'
        assert kwargs['method'] == 'POST'
        assert kwargs['payload']
        assert kwargs['queue_name'] == 'somequeue'
        assert kwargs['_target'] == 'somemodule'

    @mock.patch.object(deferred, 'DEFERRED_URL', '/defer/%s')
    def test_taskqueue_params(self):
        decorated = deferred.deferred('some/path')(top_level_function)
        deferred.defer(decorated, _target='othermodule', _queue='otherqueue')
        assert not self.defer.called
        assert self.taskqueue.called
        kwargs = self.taskqueue.call_args[1]
        assert kwargs['url'] == '/defer/some/path'
        assert kwargs['method'] == 'POST'
        assert kwargs['payload']
        assert kwargs['queue_name'] == 'otherqueue'
        assert kwargs['_target'] == 'othermodule'
