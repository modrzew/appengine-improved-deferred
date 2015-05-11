# coding: utf-8
"""
Improved deferred library for Google App Engine

It's an adapter between deferred.defer and taskqueue.
For more info and usage, see README file.
"""
from setuptools import setup


setup(
    name='appengine-improved-deferred',
    version='1.0',
    description='Improved deferred library for Google App Engine',
    author='Tomasz Modrzyński',
    py_modules=['deferred'],
    install_requires=[],
)
