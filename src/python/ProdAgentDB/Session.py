#!/usr/bin/env python

import logging

from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentCore.Codes import errors
from ProdAgentDB.Connect import connect as dbConnect

# session is a tripplet identified by an id: (connection,cursor,state)
session={}
current_session='default'

def connect(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   # check if cursor exists
   if not session.has_key(sessionID):
       session[sessionID]={}
       session[sessionID]['connection']=dbConnect(False)
       session[sessionID]['state']='connect'
       session[sessionID]['queries']=[]

def start_transaction(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException(errors[3002],3002)
   if not session[sessionID]['state']=='start_transaction':
       session[sessionID]['cursor']=session[sessionID]['connection'].cursor()
       session[sessionID]['cursor'].execute("START TRANSACTION")
       session[sessionID]['state']='start_transaction'

   
def commit(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException(errors[3002],3002)
   if session[sessionID]['state']!='commit':
       session[sessionID]['cursor'].execute("COMMIT")
       session[sessionID]['cursor'].close()
       session[sessionID]['state']='commit'
       session[sessionID]['queries']=[]
   

def rollback(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException(errors[3002],3002)
   if not session[sessionID]['state']=='start_transaction':
       raise ProdAgentException(errors[3003],3003)
   session[sessionID]['cursor'].execute("ROLLBACK")
   session[sessionID]['cursor'].close()
   session[sessionID]['state']='commit'
   session[sessionID]['queries']=[]

def close(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException(errors[3002],3002)
   session[sessionID]['connection'].close()
   del session[sessionID]

def set_current(sessionID="default"):
   global session
   global current_session

   if not session.has_key(sessionID):
       raise ProdAgentException(errors[3002],3002)
   current_session=sessionID

def execute(sqlQuery,sessionID=None):
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       logging.debug("Connection not available, trying to connect")
       connect(sessionID)
       start_transaction(sessionID)
   cursor=get_cursor(sessionID)
   try:
       cursor.execute(sqlQuery)
       session[sessionID]['queries'].append(sqlQuery)
   except:
       logging.warning("connection to database lost")
       invalidate(sessionID)
       connect(sessionID)
       start_transaction(sessionID)
       logging.warning("connection recovered")
       redo()
       cursor.execute(sqlQuery)
       session[sessionID]['queries'].append(sqlQuery)

def fetchall(sessionID=None):       
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException(errors[3002],3002)
   cursor=get_cursor(sessionID)
   return cursor.fetchall()
       
def commit_all():
   global session
   for sessionID in session.keys():
       commit(sessionID)

def close_all():
   global session
   for sessionID in session.keys():
       close(sessionID)

def rollback_all():
   global session
   for sessionID in session.keys():
       rollback(sessionID)

###########################################################
###  used only in this file
###########################################################

def redo(sessionID=None):
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       logging.debug("Connection not available, trying to connect")
       connect(sessionID)
       start_transaction(sessionID)
   cursor=get_cursor(sessionID)
   for query in session[sessionID]['queries']:
       cursor.execute(query)

def invalidate(sessionID=None):
   if sessionID==None:
       sessionID=current_session
   try:
       del session[sessionID]
   except:
       pass 

def get_cursor(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException(errors[3002],3002)
   if not session[sessionID]['state']=='start_transaction':
       start_transaction(sessionID)
   return session[sessionID]['cursor']
