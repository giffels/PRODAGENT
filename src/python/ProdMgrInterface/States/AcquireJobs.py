#!/usr/bin/env python

import logging 
import math
import time

from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdCommon.Database import Session
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

       ##### DIFFERENT HANDLERS FOR DIFFERENT REQUEST TYPES
       if stateParameters['RequestType']=='event':
           logging.debug("Acquiring jobs with a maximum size of: "+str(stateParameters['numberOfJobs'])+\
              '*'+str(stateParameters['jobCutSize'])+" events ")
           logging.debug('Request is of type event')
           eventsPerJob=int(stateParameters['numberOfJobs'])*int(stateParameters['jobCutSize']) 
           parameters={'numberOfJobs':1,\
               'prefix':'job','eventsPerJob':eventsPerJob}
       else:
           logging.debug('Request is of type file')
           parameters={'numberOfFiles':stateParameters['numberOfJobs'],\
                       'prefix':'fileJob'}

       requestURL=Request.getUrl(request_id)
       if stateParameters['stateType']=='recover':
           logging.debug("stateType is recover")
           try:
               jobs=ProdMgrAPI.retrieve(requestURL,"acquireEventJob","ProdMgrInterface")
           except ProdAgentException,ex:
               if ex['ErrorNr']==3000:
                   logging.debug("No uncommited service calls: "+str(ex))
                   stateParameters['stateType']='normal'
       elif stateParameters['stateType']=='normal':
           try:
               logging.debug("Acquiring jobs from request : "+str(request_id)+\
                   " with parameters "+str(parameters))
               jobs=ProdMgrAPI.acquireEventJob(requestURL,request_id,parameters)
               logging.debug("Acquired jobs: "+str(jobs))
           except ProdAgentException,ex:
                   Session.rollback()
                   logging.debug("Problem connecting to server "+stateParameters['ProdMgrURL']+" : "+str(ex))
                   stateParameters['requestIndex']+=1
                   Cooloff.insert(stateParameters['ProdMgrURL'],"00:10:00")
                   State.setParameters("ProdMgrInterface",stateParameters)
                   componentState="AcquireRequest"
                   State.setState("ProdMgrInterface",componentState)
                   Session.commit()
                   return componentState
           except Exception,ex:
               if str(ex).find('Connection refused')>-1:
                   logging.debug("Problem connecting to url: "+str(ex))
                   Session.rollback()
                   Cooloff.insert(stateParameters['ProdMgrURL'],"00:10:00")
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
                   logging.debug("Removing request "+str(request_id))
                   Request.rm(request_id)
                   # emit request finished event (if that is needed)
                   Request.setDone(request_id)
                   logging.debug("Checking if all jobs are finished for RequestFinished event")
                   if Request.finishedJobs(request_id):
                       logging.debug("Emitting RequestFinished event")
                       self.ms.publish("RequestFinished",request_id)
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

       ##### DIFFERENT HANDLERS FOR DIFFERENT REQUEST TYPES
       if stateParameters['RequestType']=='event':
           logging.debug("Acquired the following jobs: "+str(jobs))
           if len(jobs)!=0:
               potential_jobs=int(math.ceil(float((jobs[0]['end_event']-jobs[0]['start_event']+1))/float(stateParameters['jobCutSize'])))
               stateParameters['numberOfJobs']=stateParameters['numberOfJobs']-potential_jobs
               stateParameters['jobSpecId']=jobs[0]['jobSpecId']
           if stateParameters['numberOfJobs']<0:
               stateParameters['numberOfJobs']=0    
           # now if this request does not give us any more jobs,
           # we move on to the next request. 
           if len(jobs)==0 and stateParameters['numberOfJobs']>0:
               stateParameters['requestIndex']+=1
               Request.setDone(request_id)
               logging.debug("Checking if all jobs are finished for RequestFinished event")
               if Request.finishedJobs(request_id):
                   logging.debug("Emitting RequestFinished event")
                   self.ms.publish("RequestFinished",request_id)
       else:
           logging.debug("Acquired the following file based jobs: "+str(jobs[0]['files']))
           stateParameters['numberOfJobs']=stateParameters['numberOfJobs']-len(jobs[0]['files']) 
           if stateParameters['numberOfJobs']<0:
               stateParameters['numberOfJobs']=0    
           # now if this request does not give us any more jobs,
           # we move on to the next request. 
           if len(jobs[0]['files'])==0 and stateParameters['numberOfJobs']>0:
               stateParameters['requestIndex']+=1
               Request.setDone(request_id)
               logging.debug("Checking if all jobs are finished for RequestFinished event")
               if Request.finishedJobs(request_id):
                   logging.debug("Emitting RequestFinished event")
                   self.ms.publish("RequestFinished",request_id)
       #Now did we get enough from this request? It might that we got small left overs and we can
       #acquire more.

       # we have the jobs, lets download their job specs and if finished emit
       # a new job event.
       componentState="EvaluateJobs"
       # subtract our new found jobs from the numberOfJobs we want to acquire
       State.setParameters("ProdMgrInterface",stateParameters)
       State.setState("ProdMgrInterface",componentState)
       Session.commit()
       return componentState

registerHandler(AcquireJobs(),"AcquireJobs")







