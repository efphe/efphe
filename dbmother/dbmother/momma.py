from dbmother.pooling import *
MO_NOA    = 0     # No Action
MO_DEL    = 1     # Del Action
MO_UP     = 2     # Update Action
MO_SAVE   = 3     # Save Action
MO_LOAD   = 4     # Load Action

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

class Momma:
  def __init__(self):
    self._motherversion = 1
    self.momap= None
    self.pooling= None

  def init_mother_pooling(self, ptype, plimit, dbtype, *args, **kwargs):
    MotherPooling._dbPoolLimit= plimit
    MotherPooling._dbPoolType= ptype
    if dbtype == DB_PGRES:
      from pgres import DbIface
    elif dbtype == DB_SQLITE:
      from sqlite import DbIface
    else:
      a= 1/0
    MotherPooling._dbClass= DbIface
    MotherPooling._dbArgs= args
    MotherPooling._dbKwargs= kwargs
    MommaMomma.pooling= MotherPooling(0,0,0)

  def init_momap(self, fmap):
    try:
      self.momap= MoMap(fmap)
    except: pass

MommaMomma= Momma()

def init_mother(fmap, ptype, plimit, dbtype, *a, **kw):
  MommaMomma.init_momap(fmap)
  MommaMomma.init_mother_pooling(ptype, plimit, dbtype, *a, **kw)

def MotherSession(name= None):
  pooling= MommaMomma.pooling
  return pooling.getDb(name)

class DbMother:
  def __init__(self, store, flag= MO_NOA, session= None, tbl= None):
    if tbl:
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
    _setvalues= ses._updict(store)
    sql= 'UPDATE %s set %s where id = %d' % (self.tableName, _setvalues, store['id'])
    ses.oc_query(sql, store)

  def insert(self, d= {}):
    ses= self.session
    store= self.store
    store.update(d)
    _vl, vlvl= ses._insict(store)
    sql= 'INSERT INTO %s (%s) VALUES (%s)' % (self.tableName, _vl, vlvl)
    d= ses.insert(sql, store, self.tableName)
    store.update(d)

  def load(self, d= {}, fields= []):
    ses= self.session
    store= self.store
    store.update(d)
    what, ftr= ses._selict(store, fields)
    sql= 'select %s from %s where %s' % (what, self.tableName, ftr)
    d= ses.or_query(sql, store)
    store.update(d)

  def delete(self, d= {}):
    ses= self.session
    store= self.store
    store.update(d)
    ftr= ses._delict(store)
    sql= 'delete from %s where %s' % (self.tableName, ftr)
    ses.oc_query(sql, store)

