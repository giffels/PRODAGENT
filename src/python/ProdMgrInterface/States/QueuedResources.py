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


class QueuedResources(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: QueuedResources")
       stateParameters=State.get("ProdMgrInterface")['parameters']

       logging.debug("Retrieving a message (if available)")
       message=MessageQueue.retrieve('ProdMgrInterface','retrieveWork',stateParameters['queueIndex'])
       if message==[]:
          logging.debug("No messages currently available for processing")
          componentState="AcquireRequest"
          # we use this to not have to call this again during handling of the (internal) message (from the 
          # message service
          stateParameters['queueIndex']=-1
          State.setState("ProdMgrInterface","AcquireRequest")
          Session.commit()
          return componentState
       # we go one, remove it from the queue and restore to
       # the state we left it in.
       MessageQueue.rm(message[0]['id'])
       currentIndex=stateParameters['queueIndex']
       stateParameters.update(message[0]['parameters'])
       stateParameters['stateType']='queue'
       stateParameters['queueIndex']=currentIndex+1
       State.setParameters("ProdMgrInterface",stateParameters)
       State.setParameters("ProdMgrInterface",message[0]['state'])
       Session.commit()
       # note that there are only 2 states we can go to: "AcquireJobs" and "DownloadJobSpecs"
       return message[0]['state']

registerHandler(QueuedResources(),"QueuedResources")







