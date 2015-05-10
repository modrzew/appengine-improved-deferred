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

    def test_no_module_attribute(self):
        with pytest.raises(deferred.InvalidPath):
            deferred._load('tests.something_nonexistent')

    def test_no_class_attribute(self):
        with pytest.raises(deferred.InvalidPath):
            deferred._load('tests.Parent.nonexistent')


class GenerateHashTests(unittest.TestCase):
    def test_ok(self):
        args = (1, 2, 'some', 'thing')
        kwargs = {
            'another': 'thing',
            'value': 42,
        }
        expected = hashlib.sha256(pickle.dumps((args, kwargs))).hexdigest()
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
