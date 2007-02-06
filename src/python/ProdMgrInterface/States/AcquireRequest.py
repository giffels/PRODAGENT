#!/usr/bin/env python

import logging 

from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdCommon.Database import Session
from ProdMgrInterface import Cooloff
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
       componentState=State.get("ProdMgrInterface")
       #check if we reached our threshold. if so quit.
       if int(componentState['parameters']['numberOfJobs']==0):
           State.setState("ProdMgrInterface","Cleanup")
           Session.commit()
           return 'Cleanup'
       # get request with highest priority:
       logging.debug("Getting request with index: "+str(componentState['parameters']['requestIndex']))
       requestIndex=componentState['parameters']['requestIndex']
       request=Request.getHighestPriority(requestIndex)
       # if this is true we have no more requests to check:
       if request=={}:
           State.setState("ProdMgrInterface","Cleanup")
           Session.commit()
           return "Cleanup"
       # if this url is available in the cooloff table do not
       # use it :
       if request!={}:
           while Cooloff.hasURL(request['url']) :
               requestIndex=requestIndex+1
               request=Request.getHighestPriority(requestIndex)
               if request=={}:
                   break
       # check if there are requests left:
       if (request=={}):
           State.setState("ProdMgrInterface","Cleanup")
           Session.commit()
           logging.debug("We have no more requests in our queue for allocations"+\
               " and jobs, bailing out")
           return "Cleanup"
       logging.debug("Found request: "+str(request['id']))
       # set parameters and state and commit for next session
       componentState['parameters']['RequestID']=request['id']
       componentState['parameters']['ProdMgrURL']=request['url']
       componentState['parameters']['RequestType']=request['type']
       componentState['parameters']['requestIndex']=requestIndex
       State.setParameters("ProdMgrInterface",componentState['parameters'])
       State.setState("ProdMgrInterface","AcquireJobs")
       Session.commit()
       return "AcquireJobs"


registerHandler(AcquireRequest(),"AcquireRequest")







