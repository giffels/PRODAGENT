#!/usr/bin/env python

import logging 
import time

from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB import Session
from ProdMgrInterface import Cooloff
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
       logging.debug("State parameters: "+str(stateParameters))
       request_id=stateParameters['RequestID']
       logging.debug("Acquiring jobs with a maximum size of: "+str(stateParameters['jobSize'])+" events ")
       parameters={'numberOfJobs':stateParameters['numberOfJobs'],\
           'prefix':'job','event_count':int(stateParameters['jobSize'])}
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
           except ProdAgentException,ex:
                   Session.rollback()
                   logging.debug("Problem connecting to server "+stateParameters['ProdMgrURL']+" : "+str(ex))
                   stateParameters['requestIndex']+=1
                   State.setParameters("ProdMgrInterface",stateParameters)
                   componentState="AcquireRequest"
                   State.setState("ProdMgrInterface",componentState)
                   Session.commit()
                   return componentState
           except Exception,ex:
               if str(ex).find('Connection refused')>-1:
                   logging.debug("Problem connecting to url: "+str(ex))
                   Session.rollback()
                   componentState="AcquireRequest"
                   State.setState("ProdMgrInterface",componentState)
                   stateParameters['requestIndex']+=1
                   State.setParameters("ProdMgrInterface",stateParameters)
                   Session.commit()
                   return componentState
               if ex.faultCode==2009:
                   Session.rollback()
                   logging.debug("This prodagent is set in cooloff state with "+stateParameters['ProdMgrURL'])
                   logging.debug("Trying to acquire other allocations")
                   Cooloff.insert(stateParameters['ProdMgrURL'],"00:10:00")
                   stateParameters['requestIndex']+=1
                   State.setParameters("ProdMgrInterface",stateParameters)
                   componentState="AcquireRequest"
                   State.setState("ProdMgrInterface",componentState)
                   Session.commit()
                   return componentState
               if ex.faultCode==2002 or ex.faultCode==2032:
                   Session.rollback()
                   logging.debug("Request: "+str(request_id)+" does not exists or has finished")
                   logging.debug("Removing request")
                   Request.rm(request_id)
                   State.setParameters("ProdMgrInterface",stateParameters)
                   componentState="AcquireRequest"
                   State.setState("ProdMgrInterface",componentState)
                   Session.commit()
                   return componentState
               raise
       stateParameters['stateType']='normal'
       stateParameters['jobIndex']=0
       Job.insert('requestLevel',jobs,request_id,requestURL)
       ProdMgrAPI.commit()
       logging.debug("Acquired the following jobs: "+str(jobs))
       # we have the jobs, lets download there job specs and if finished emit
       # a new job event.
       componentState="EvaluateJobs"
       # subtract our new found jobs from the numberOfJobs we want to acquire
       stateParameters['numberOfJobs']=stateParameters['numberOfJobs']-len(jobs) 
       State.setParameters("ProdMgrInterface",stateParameters)
       State.setState("ProdMgrInterface",componentState)
       Session.commit()
       return componentState

registerHandler(AcquireJobs(),"AcquireJobs")







