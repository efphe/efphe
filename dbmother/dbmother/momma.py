_commajoin= ','.join
from dbmother.pooling import *
#from time import time
#MO_NOA    = 0     # No Action
#MO_DEL    = 1     # Del Action
#MO_UP     = 2     # Update Action
#MO_SAVE   = 3     # Save Action
#MO_LOAD   = 4     # Load Action

#class PerfectSleeper:
  #lastrelase= None
  #@staticmethod
  #def release_time():
    #t= '%.4f' % time()
    #while t == PerfectSleeper.lastrelase:
      #t= '%.4f' % time()
    #PerfectSleeper.lastrelase= t

class MommaSql:
  argFrmt= None
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
    sl= self._equalKeys(d, skipid= 0)
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


class MoMap:
  def __init__(self, fmap):
    fil= open(fmap, 'rb')
    import cPickle
    map_dicts= cPickle.load(fil)
    self._map_file= map_file
    self._map_fields= map_dicts['K']
    self._map_pkeys= map_dicts['P']
    self._map_children= map_dicts['C']
    self._map_rels= map_dicts['R']
    fil.close()

class MommaRoot:
  def __init__(self):
    self.momap= None
    self.pooling= None

  def init_mother_pooling(self, ptype, plimit, dbtype, *args, **kwargs):
    MotherPooling._dbPoolLimit= plimit
    MotherPooling._dbPoolType= ptype
    if dbtype == DB_PGRES:
      from pgres import DbIface
      MommaSql.argFrmt= '%%(%s)s'
    elif dbtype == DB_SQLITE:
      from sqlite import DbIface
      MommaSql.argFrmt= ':s'
    else:
      a= 1/0
    MotherPooling._dbClass= DbIface
    MotherPooling._dbArgs= args
    MotherPooling._dbKwargs= kwargs
    Momma.pooling= MotherPooling(0,0,0)

  def init_momap(self, fmap):
    try:
      self.momap= MoMap(fmap)
    except: pass

#try:
  #from twisted.spread import pb
  #class MommaRootPb(pb.Root):
    #def __init__(self):
      #self.sessionMap= {}
      #self.pooling= Momma.pooling
    #def remote_get_session(self, name= None):
      #ses= self.pooling.getDb(name)
      #tok= PerfectSleeper.release_time()
      #self.sessionMap[tok]= ses
      #return tok
    #def remote_oc_query(self, tok, q, d= None):
      #ses= self.sessionMap[tok]
      #return ses.oc_query(q, d)
    #def remote_or_query(self, tok, q, d= None):
      #ses= self.sessionMap[tok]
      #return ses.or_query(q, d)
    #def remote_mr_query(self, tok, q, d= None):
      #ses= self.sessionMap[tok]
      #return ses.mr_query(q, d)
    #def remote_ov_query(self, tok, q, d= None):
      #ses= self.sessionMap[tok]
      #return ses.ov_query(q, d)
    #def remote_endSession(self, tok):
      #pass
    #def remote_rollback(self, tok):
      #pass
    #def remote_commit(self, tok):
      #pass

  #class MotherSessionPb:
    #def __init__(self, name= None):
      #self.name= name
      #self.tok= self.server.callRemote('get_session', name)
    #def oc_query(self, q, d):
      #tok= self.tok
      #return self.server.callRemote('oc_query', tok, q, d)
    #def ov_query(self, q, d):
      #tok= self.tok
      #return self.server.callRemote('ov_query', tok, q, d)
    #def or_query(self, q, d):
      #tok= self.tok
      #return self.server.callRemote('or_query', tok, q, d)
    #def mr_query(self, q, d):
      #tok= self.tok
      #return self.server.callRemote('mr_query', tok, q, d)
#except: pass


#def MotherSession(name= None, remote= False):
  #if 1 or not remote:
    #pooling= Momma.pooling
    #return pooling.getDb(name)
  #return MotherSessionPb(name)


Momma= MommaRoot()
def MotherSession(name= None):
  pooling= Momma.pooling
  return pooling.getDb(name)

class WMotherSession(object):
  def __init__(self, name= None):
    self.name= name
    self.session= None
  def __enter__(self):
    ses= MotherSession(self.name)
    self.session= ses
    return ses
  def __exit__(self, type, value, traceback):
    if type:
      self.session.rollback()
    self.session.endSession()
    return False

#def init_mother(ptype, plimit, dbtype, async, *a, **kw):
  #Momma.init_mother_pooling(ptype, plimit, dbtype, *a, **kw)
  #def _fses(name): 
    #if async:
      #return MotherSessionGen(name, 1)
    #return MotherSessionGen(name)
  #global MotherSession
  #MotherSession= _fses

class MotherInitializer:
  def init_db(self, ptype, plimit, dbtype, *a, **kw):
    Momma.init_mother_pooling(ptype, plimit, dbtype, *a, **kw)
  #def run_async(self, host= 'localhost', port= 91823):
    #from twisted.spread import pb
    #from twisted.internet import reactor
    #serverfactory = pb.PBServerFactory(MommaRootPb())
    #reactor.listenTCP(port, serverfactory)
    #reactor.run()


class DbMother(MommaSql):
  def __init__(self, session, tbl, store= {}):
    self.tableName= tbl
    self.store= store
    self.session= session
    self.moved= []

  def setFields(self, d):
    self.store.update(d)
  def setField(self, k, v):
    self.store[k]= v
  def getFields(self, unsafe= 1):
    if unsafe:
      return self.store
    return self.store.copy()
  def getField(self, k, default= None):
    return self.store.get(k, default)

  def update(self, d= {}):
    ses= self.session
    store= self.store
    store.update(d)
    vls= self._updict(store)
    sql= 'UPDATE %s set %s where id = %d' % (self.tableName, vls, store['id'])
    ses.oc_query(sql, store)
    return self

  def insert(self, d= {}):
    ses= self.session
    store= self.store
    store.update(d)
    _vl, _vlv= self._insict(store)
    sql= 'INSERT INTO %s (%s) VALUES (%s)' % (self.tableName, _vl, _vlv)
    d= ses.insert(sql, store, self.tableName)
    store.update(d)
    return self

  def load(self, d= {}, fields= [], safe= False):
    ses= self.session
    store= self.store
    store.update(d)
    what, ftr= self._selict(store, fields)
    sql= 'select %s from %s where %s' % (what, self.tableName, ftr)
    if safe:
      dd= ses.mr_query(sql, store)
      assert len(dd) < 2
      if dd: dd= dd[0]
      else: dd= {}
    else:
      dd= ses.or_query(sql, store)
    store.update(dd)
    return self

  def delete(self, d= {}):
    ses= self.session
    store= self.store
    store.update(d)
    ftr= self._delict(store)
    sql= 'delete from %s where %s' % (self.tableName, ftr)
    ses.oc_query(sql, store)
    return self

