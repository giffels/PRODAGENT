#!/usr/bin/env python

import logging

from ProdAgentCore.ProdAgentException import ProdAgentException
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
       session[sessionID]['cursor']=session[sessionID]['connection'].cursor()
       session[sessionID]['state']='connect'

def start_transaction(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException("start_transaction: First create connection",3002)
   session[sessionID]['cursor'].execute("START TRANSACTION")
   session[sessionID]['state']='start_transaction'

def get_cursor(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException("get_cursor: First create connection",3002)
   if not session[sessionID]['state']=='start_transaction':
       raise ProdAgentException("get_cursor: First start transaction",3003)
   return session[sessionID]['cursor']
   
def commit(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException("commit: First create connection",3002)
   session[sessionID]['cursor'].execute("COMMIT")
   session[sessionID]['state']='commit'

def rollback(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException("commit: First create connection",3002)
   if not session[sessionID]['state']=='start_transaction':
       raise ProdAgentException("commit: First start transaction",3003)
   session[sessionID]['cursor'].execute("ROLLBACK")
   session[sessionID]['state']='commit'

def close(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdAgentException("close: First create connection",3002)
   session[sessionID]['cursor'].close()
   session[sessionID]['connection'].close()
   del session[sessionID]

def set_current(sessionID="default"):
   global session
   global current_session

   if not session.has_key(sessionID):
       raise ProdAgentException("close: First create connection",3002)
   current_session=sessionID

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

