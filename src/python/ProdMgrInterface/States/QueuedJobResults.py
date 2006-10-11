#!/usr/bin/env python

import logging 

from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB import Session
from ProdMgrInterface import Allocation
from ProdMgrInterface import MessageQueue
from ProdMgrInterface import Job
from ProdMgrInterface import Request
from ProdMgrInterface import State
from ProdMgrInterface.Registry import registerHandler
from ProdMgrInterface.States.StateInterface import StateInterface 
import ProdMgrInterface.Interface as ProdMgrAPI


class QueuedJobResults(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: QueuedJobResults")
       stateParameters=State.get("ProdMgrInterface")['parameters']
       ignoreUrl={}


       for report_type in ['reportJobSuccess','reportJobFailure']:
           messages=MessageQueue.retrieve('ProdMgrInterface',report_type)
           for message in messages:
               if not ignoreUrl.has_key(message['server_url']):
                   try:
                       logging.debug("Attempting to connect to server : "+message['server_url'])
                       #NOTE: the service interaction should be done more robust
                       finished=ProdMgrAPI.releaseJob(message['server_url'],str(message['parameters']['jobSpecId']),\
                           int(message['parameters']['events']),"ProdMgrInterface")
                       if finished:
                          # if the request is finished, remove it from our queue
                          # NOTE: perhaps we should send a kill event for remaining jobs?
                          request_id=message['parameters']['jobSpecId'].split('/')[1]
                          Request.rm(request_id)
                          Allocation.rm(request_id)
                       MessageQueue.remove(message['id'])
                       allocation_id=message['parameters']['jobSpecId'].split('/')[1]+'/'+\
                       message['parameters']['jobSpecId'].split('/')[3]
                       Allocation.setState('prodagentLevel',allocation_id,'idle')
                       Session.commit()
                   except ProdAgentException, ex:
                       # there is a problem connecting wrap up all relevant information and put
                       # it in a queue for later inspection
                       MessageQueue.reinsert(message['id'],'00:0:10')
                       logging.debug("Problem connecting to server "+message['server_url']+" : "+str(ex))
                       Session.commit()
                       ignoreUrl[message['server_url']]='ignore'
                   except Exception,ex:
                       if ex.faultCode==2000:
                          logging.debug("This job was already released. Taking appropiate actions")
                       #NOTE: distinguish between different exceptions
               logging.debug("Retrieve next message in queue")
       logging.debug("Examined all messages in queue. Moving to next state")
       if stateParameters['jobType']=='success':
           return 'ReportJobSuccess'
       if stateParameters['jobType']=='failure':
           return 'ReportJobFailure'

registerHandler(QueuedJobResults(),"QueuedJobResults")







