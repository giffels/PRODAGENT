#!/usr/bin/env python

"""
_Coolof_

Methods that keep track of what ProdMgrs charged this ProdAgent
with a coolof penalty.

"""

__revision__  =  "$Id: Cooloff.py,v 0.01 2007/05/31 fvlingen Exp $"
__version__  =  "$Revision: 0.00 $"
__author__  =  "fvlingen@caltech.edu"

from ProdCommon.Database import Session

def hasURL(url):
    sqlStr = """SELECT COUNT(*) FROM pm_cooloff WHERE url = "%s";
        """ %(url)
    Session.execute(sqlStr)
    rows = Session.fetchall()
    if rows[0][0] == 0:
        return False
    return True

def insert(url,delay = "00:00:00"):
    sqlStr = """INSERT INTO pm_cooloff(url,delay) 
        VALUES("%s","%s") ON DUPLICATE KEY UPDATE delay = "%s"; 
        """ %(url,delay,delay)
    Session.execute(sqlStr)

def remove():
   sqlStr = """DELETE FROM pm_cooloff WHERE 
       ADDTIME(log_time,delay)<=  CURRENT_TIMESTAMP """
   Session.execute(sqlStr)
