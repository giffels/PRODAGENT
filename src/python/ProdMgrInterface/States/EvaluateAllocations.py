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


class EvaluateAllocations(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       componentStateInfo=State.get('ProdMgrInterface')
       logging.debug("Executing state: EvaluateAllocations ")
       if Allocation.size("messageLevel")<int(componentStateInfo['parameters']['numberOfJobs']):
           if componentStateInfo['parameters']['queueIndex']>-1:
               State.setState("ProdMgrInterface","QueuedResources") 
               Session.commit()
               return 'QueuedResources'
           # reset the request index as this is the first time we 
           # start to use it:
           componentStateInfo['parameters']['requestIndex']=0
           State.setParameters("ProdMgrInterface",componentStateInfo['parameters'])
           State.setState("ProdMgrInterface","AcquireRequest") 
           Session.commit()
           return 'AcquireRequest'
       else:
           State.setState("ProdMgrInterface","Cleanup")
           Session.commit()
           return 'Cleanup'
 

registerHandler(EvaluateAllocations(),"EvaluateAllocations")







