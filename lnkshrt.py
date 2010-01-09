#!/usr/bin/python

# License: do with this code what you want
# Author: Federico Tomassini, aka efphe
#   Thanks to: http://wubook.net/, http://en.wubook.net/

# *** How To Use ***
#
#    Simply put this file on your PATH. Run it as:
#
#        lnkshrt.py `url`

import urllib as _
import sys as __

def _g(u):
  e= _.urlencode({'url': u})
  f= _.urlopen('http://dr.tl/?module=ShortURL&file=Add&mode=API', e)
  return f.read()

if __name__ == '__main__':
  try:
    url= __.argv[1]
    f= _g(url)
    print f
  except:
    print \
"""Usage:
  lnkshrt.py url

You must specify `url`"""
