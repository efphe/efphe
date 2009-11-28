_commajoin= ','.join
class IMotherDb:

  def set_name(self, name):
    self.session_name= name

  def oc_query(self, s, filter= None):
    self._qquery(s, filter)

  def mr_query(self, s, filter= None):
    return self._gquery(s, filter)

  def or_query(self, s, filter= None):
    res= self.mr_query(s, filter)
    assert len(res) == 1
    return res[0]

  def ov_query(self, s, filter= None):
    res= self.or_query(s, filter)
    res= res.values()
    assert len(res) == 1
    return res[0]

  def mq_query(self, s, l):
    self._mqquery(s, l)

  def mg_query(self, s, l):
    self._mgquery(s, l)

  def _equalKeys(self, d, skipid= 1):
    sl= []
    af= self.argFrmt
    for k, v in d.iteritems():
      if k == 'id' and skipid: continue
      sl.append('%s= %s' % (str(k), af % k))
    return sl

  def _updict(self, d):
    sl= self._equalKeys(d)
    return _commajoin(sl)

  def _selict(self, d, f):
    sl= self._equalKeys(d)
    what= f and _commajoin(f) or '*'
    return what, _commajoin(sl)

  def _insict(self, d):
    sls= []
    sld= []
    af= self.argFrmt
    for k, v in d.iteritems():
      if k == 'id': continue
      sls.append(k)
      sld.append(af % k)
    return _commajoin(sls), _commajoin(sld)

  def _delict(self, d):
    if 'id' in d:
      return _commajoin(self._equalKeys({'id': d['id']}, skipid= 0))
    return _commajoin(self._equalKeys(d))

