# Improved deferred for App Engine

[![Build Status](https://travis-ci.org/modrzew/appengine-improved-deferred.svg)](https://travis-ci.org/modrzew/appengine-improved-deferred)
[![Coverage Status](https://coveralls.io/repos/modrzew/appengine-improved-deferred/badge.svg)](https://coveralls.io/r/modrzew/appengine-improved-deferred)

This is homemade implementation of `deferred.defer` for
[Google App Engine](https://cloud.google.com/appengine/), specifically for
Python runtime.

## Why?

Original defer, while being extremely useful in the early stage of development
(you just *defer the function and forget about it*), is not very friendly to
your app when it comes to monitoring resources usage, or finding any particular
error in the logs. This is how your logs screen will look like after a while:

```
    [200] /_ah/queue/deferred
    [400] /_ah/queue/deferred
    [200] /_ah/queue/deferred
    [200] /_ah/queue/deferred
    [200] /_ah/queue/deferred
    [500] /_ah/queue/deferred
    [200] /_ah/queue/deferred
    [200] /_ah/queue/deferred
    [503] /_ah/queue/deferred
    [200] /_ah/queue/deferred
    ...
```

And in the time of writing this code, [Cloud
Trace](https://cloud.google.com/tools/cloud-trace) was in beta phase. It's an
tool for App Engine for creating reports about resource usage on any particular
handler in your app. It's not very helpful if all of your handlers are
identified by `/_ah/queue/deferred`, is it?

Improved deferred turns your logs into something like this:

```
    [200] /deferred/user/create
    [400] /deferred/item/remove
    [200] /deferred/cron_job
    [200] /deferred/user/index
    [200] /deferred/item/add
    [500] /deferred/item/add
    [200] /deferred/user/index
    [200] /deferred/user/create
    [503] /deferred/cron_job
    [200] /deferred/relationship/create
    ...
```



Now it's tremendously easy to find the request you had in your mind!

## Usage

Instead of importing `google.appengine.ext.deferred`, just import `deferred`
module from this repository!

```python
import deferred


def some_function():
    pass


# Use it as you normally would use defer
deferred.defer(some_function, 1, 2, some='kwarg', _target='module')


# And just decorate your functions with @deferred for magic!
@deferred.deferred('functions/magic')
def magic_function():
    pass


# It's simple!
deferred.defer(magic_function, 3, 4, another='kwarg', _queue='not-default')
```

## Where is the magic?

`@deferred` decorator defers your functions using `taskqueue.add` and special
handler instead of just calling `deferred.defer`. That gives you much more
control over what is executed, how and when. And how it's displayed in the logs
or in the reports.

Another thing worth mentioning is that Datastore is never used here. Standard
deferred library pickles your task payload, and, if it's bigger than payload
limit for the taskqueue (100KB at the moment of writing this) it puts it to
Datastore. Which slows your request down. By using `@deferred`, you need to
monitor what you pass to the deferred tasks (rule of thumb is, you should pass
urlsafes/IDs instead of actual objects).

## License

See [LICENSE.md](LICENSE.md).
