import hashlib
import unittest

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
        assert func is Parent.function

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
        args = (1, 2, 'some', 'thing', Parent.function)
        kwargs = {
            'another': 'thing',
            'value': 42,
            'inner': Parent.Inner,
        }
        expected = hashlib.sha256(pickle.dumps((args, kwargs))).hexdigest()
        result = deferred._generate_hash(args, kwargs)
        assert result == expected


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
        assert self.executor.call_count == 6

    @mock.patch.object(deferred, 'MAX_RETRIES', 5)
    @mock.patch(deferred, 'logging')
    def test_already_exists(self, m_l):
        error = taskqueue.TaskAlreadyExistsError
        self.executor.side_effect = [DeadlineExceededError] * 3 + [error]
        deferred._execute(self.executor, 1, 2, 'a', 'mama', some='thing')
        assert self.executor.call_count == 4
        assert m_l.warning.called


    @mock.patch.object(deferred, 'MAX_RETRIES', 5)
    @mock.patch(deferred, 'logging')
    def test_tombstoned(self, m_l):
        error = taskqueue.TombstonedTaskError
        self.executor.side_effect = [DeadlineExceededError] * 3 + [error]
        deferred._execute(self.executor, 1, 2, 'a', 'mama', some='thing')
        assert self.executor.call_count == 4
        assert m_l.warning.called

    @mock.patch.object(deferred, 'MAX_RETRIES', 5)
    @mock.patch(deferred, 'logging')
    def test_too_large(self, m_l):
        error = taskqueue.TaskTooLargeError
        self.executor.side_effect = [DeadlineExceededError] * 3 + [error]
        deferred._execute(self.executor, 1, 2, 'a', 'mama', some='thing')
        assert self.executor.call_count == 4
        assert m_l.exception.called
