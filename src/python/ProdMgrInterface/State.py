#!/usr/bin/env python

"""
_State_

Methods that retrieve state information from the database.
If the ProdMgrInterface fails and their has been a commit 
while handling an event the state has been stored and needs to be retrieved.
"""

__revision__ = "$Id: State.py,v 0.01 2007/05/31 fvlingen Exp $"
__version__ = "$Revision: 0.00 $"
__author__ = "fvlingen@caltech.edu"


import cPickle
import logging

from ProdCommon.Database import Session

def get(id):
   sqlStr='SELECT id,state,parameters FROM pm_state WHERE '+\
          ' id="'+str(id)+'";'
   Session.execute(sqlStr)
   rows=Session.fetchall()
   if len(rows)==0:
       return {}
   result={}
   result['id']=rows[0][0]
   result['state']=rows[0][1]
   result['parameters']=cPickle.loads(rows[0][2])
   return result

def insert(id,state,parameters):
   sqlStr="""INSERT INTO pm_state(id,state,parameters)
       VALUES("%s","%s","%s") """ %(id,state,cPickle.dumps(parameters))
   Session.execute(sqlStr)


def setParameters(id,parameters={}):
    sqlStr='UPDATE pm_state SET parameters="'+str(cPickle.dumps(parameters))+\
        '" WHERE id="'+str(id)+'" ;'
    Session.execute(sqlStr)

def setState(id,state):
    sqlStr='UPDATE pm_state SET state="'+state+'";'
    Session.execute(sqlStr)

