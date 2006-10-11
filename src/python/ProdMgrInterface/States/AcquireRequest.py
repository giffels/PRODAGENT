#!/usr/bin/env python

import logging 

from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB import Session
from ProdMgrInterface import Allocation
from ProdMgrInterface import Cooloff
from ProdMgrInterface import MessageQueue
from ProdMgrInterface import Job
from ProdMgrInterface import Request
from ProdMgrInterface import State
from ProdMgrInterface.Registry import registerHandler
from ProdMgrInterface.States.StateInterface import StateInterface 


class AcquireRequest(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: AcquireRequest")
       # remove potential server urls from the cooloff state
       Cooloff.remove()
       Session.commit()
       # get request with highest priority:
       componentState=State.get("ProdMgrInterface")
       requestIndex=componentState['parameters']['requestIndex']
       request=Request.getHighestPriority(requestIndex)
       # if this is true we have no more requests to check:
       if request=={}:
           State.setState("ProdMgrInterface","Cleanup")
           Session.commit()
           return "Cleanup"
 
       # if this url is available in the message queue do not
       # use it  
       while MessageQueue.hasURL(request['url']) and\
           Request.size()<(requestIndex+1):
           requestIndex=requestIndex+1
           request=Request.getHighestPriority(requestIndex)

       # if this url is available in the cooloff table do not
       # use it either:
       while Cooloff.hasURL(request['url']) and\
           Request.size()<(requestIndex+1):
           requestIndex=requestIndex+1
           request=Request.getHighestPriority(requestIndex)

       # check if there are requests left:
       if (Request.size())<(requestIndex+1):
           State.setState("ProdMgrInterface","Cleanup")
           Session.commit()
           logging.debug("We have no more requests in our queue for allocations"+\
               " and jobs, bailing out")
           return "Cleanup"

       # set parameters and state and commit for next session
       componentState['parameters']['RequestID']=request['id']
       componentState['parameters']['ProdMgrURL']=request['url']
       componentState['parameters']['requestIndex']=requestIndex
       State.setParameters("ProdMgrInterface",componentState['parameters'])
       State.setState("ProdMgrInterface","AcquireAllocations")
       Session.commit()
       return "AcquireAllocations"


registerHandler(AcquireRequest(),"AcquireRequest")







