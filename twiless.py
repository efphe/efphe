import logging
import stackless
from twisted.internet import defer
_stackless_thread_channel= stackless.channel()

""" This module allows to run normal code suites as deferred objects,
    without creating expensive threads, but using stackless.

    There is a limit: deferred suites are FIFO'ed and not concurrent
"""

def _underground(d, f, *a, **kw):
  """ This function runs f() on stackless, making sure that,
      when done, the deferred d will call back."""
  md= defer.maybeDeferred(f, *a, **kw)
  def _cb(x):
    reactor.callFromThread(d.callback, x)
  def _eb(x):
    reactor.callFromThread(d.errback, x)
  md.addCallback(_cb)
  md.addErrback(_eb)

def stackless_thread():
  """ Runs the Stackless thread. A Channel is created, which
      wait for activities. Once had it, it launch a function
      on background.

      You must run a stackless_thread doing something like:

        reactor.callInThread(stackless_thread)

      After that, to defer a function f, simply do:
        
        df= send_stackless_activity(f, *a, **kw)

      df is a deferred."""
  while 1:
    try:
      d, f, a, kw= _stackless_thread_channel.receive()
      t= stackless.tasklet(_underground)
      t(d, f, a, kw)
      stackless.schedule()
    except Exception, ss:
      print 1
      logging.error(str(ss))

def send_stackless_activity(f, *a, **kw):
  """ This allows to launch a function in an async way. A Deferred is returned """
  d= defer.Deferred()
  t= stackless.tasklet(_stackless_thread_channel.send)
  t((d, f, a, kw))
  stackless.schedule()
  return d
