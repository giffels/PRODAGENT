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
       logging.debug("Executing state: EvaluateAllocations "+str(componentStateInfo))
       if Allocation.size("messageLevel")<int(componentStateInfo['parameters']['numberOfJobs']):
           State.setState("ProdMgrInterface","AcquireRequest") 
           Session.commit()
           return 'AcquireRequest'
       else:
           State.setState("ProdMgrInterface","Cleanup")
           Session.commit()
           return 'Cleanup'
 

registerHandler(EvaluateAllocations(),"EvaluateAllocations")







