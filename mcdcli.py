from cPickle import dumps, loads
from binascii import b2a_base64, a2b_base64
from memcache import Client as _mClient
from hashlib import md5

class IMemCache(_mClient):

  def __init__(self):
    _mClient.__init__(self, ['127.0.0.1:11211'])

  def _sdump(self, ob, split= 0):
    buf= dumps(ob)
    return b2a_base64(buf)

  def _sload(self, buf):
    buf= a2b_base64(buf)
    return loads(buf)

  def _getkey(self, *a, **kw):
    k= '%d%s%s' % (id(self.__class__), str(a), str(kw))
    return md5(k).hexdigest()

  def memget(self, *a, **kw):
    k= self._getkey(*a, **kw)
    vl= self.get(k)
    if vl is None:
      vl= self._load_values(*a, **kw)
      svl= self._sdump(vl)
      self.set(k, svl)
      return vl
    else:
      return self._sload(vl)

  def memreset(self, *a, **kw):
    k= self._getkey(*a, **kw)
    self.delete(k)

class Bar:
  def __init__(self, a= 1, b= 2):
    self.a= a
    self.b= b

class FooCache(IMemCache):

  """ Just Implement _load_values() to define how your values have to be
      loaded. Arguments passed to _load_values() are the same arguments
      passed to memget().

      Note: memcached keys depend on *a, **kw passed to memget().

      Marshalling is automatic. Use what You want."""

  def _load_values(self, *a, **kw):
    print 'Loading values...'
    return (1,2,3), Bar()

im= IMemCache()
im= FooCache()
