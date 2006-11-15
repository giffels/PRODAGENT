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


class AcquireJobs(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: AcquireJobs")
       stateParameters=State.get("ProdMgrInterface")['parameters']
       request_id=stateParameters['RequestID']
       logging.debug("stateType :"+stateParameters['stateType'])

       # if it is queue, restore the message and request level
       # allocations 
       if stateParameters['stateType']=='queue':
           logging.debug("stateType is queue")
           Allocation.mv('requestLevelQueued','requestLevel',request_id)
           Allocation.mv('messageLevelQueued','messageLevel',request_id)
           Session.commit()
           # now everything is back to normal, we do not have to recover
           # as making the call is what went wrong.
           stateParameters['stateType']=='normal'
          
       parameters={'numberOfJobs':Allocation.size('requestLevel'),
           'prefix':'job'}
       requestURL=Request.getUrl(request_id)

       if stateParameters['stateType']=='recover':
           logging.debug("stateType is recover")
           try:
               jobs=ProdMgrAPI.retrieve(requestURL,"acquireJob","ProdMgrInterface")
           except ProdAgentException,ex:
               if ex['ErrorNr']==3000:
                   logging.debug("No uncommited service calls: "+str(ex))
                   stateParameters['stateType']='normal'
       elif stateParameters['stateType']=='normal':
           try:
               logging.debug("Acquiring jobs from request : "+str(request_id)+\
                   " with parameters "+str(parameters))
               jobs=ProdMgrAPI.acquireJob(requestURL,request_id,parameters)
               logging.debug("Acquired jobs: "+str(jobs))
           except Exception,ex:
               # there is a problem connecting wrap up all relevant information and put
               # it in a queue for later insepection
               # NOTE: move the allocations associated from message and request level to
               # NOTE: something else
               Session.rollback()
               Allocation.mv('requestLevel','requestLevelQueued',request_id)
               Allocation.mv('messageLevel','messageLevelQueued',request_id)
               MessageQueue.insert("ProdMgrInterface","retrieveWork",requestURL,"AcquireJobs",stateParameters)
               componentState="AcquireRequest"
               State.setState("ProdMgrInterface",componentState)
               Session.commit()
               return componentState

       stateParameters['stateType']='normal'
       stateParameters['jobIndex']=0
       Job.insert('requestLevel',jobs,request_id)
       # we can now savely remove the request level allocation queue
       Allocation.rm('requestLevel')
       ProdMgrAPI.commit()
       logging.debug("Acquired the following jobs: "+str(jobs))
       # we have the jobs, lets download there job specs and if finished emit
       # a new job event.
       componentState="EvaluateJobs"
       State.setParameters("ProdMgrInterface",stateParameters)
       State.setState("ProdMgrInterface",componentState)
       Session.commit()
       return componentState

registerHandler(AcquireJobs(),"AcquireJobs")







