#!/usr/bin/env python

import logging 
import os

from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdCommon.Database import Session
from ProdMgrInterface import MessageQueue
from ProdAgent.WorkflowEntities import Allocation
from ProdAgent.WorkflowEntities import File
from ProdAgent.WorkflowEntities import Job 
from ProdAgent.WorkflowEntities import Workflow
import ProdMgrInterface.Interface as ProdMgrAPI

ms=None

def handleJob(job_spec_id):

   Job.remove(job_spec_id)
   logging.debug("Evaluate allocation associated to the job "+str(job_spec_id))
   jobId=Allocation.convertJobID(job_spec_id)
   logging.debug("Associated allocation is: "+str(jobId)) 
   if Allocation.isJobsFinished(jobId):
       logging.debug("Retrieve number of processed events")
       allocation=Allocation.get(jobId)
       logging.debug("All jobs associated to allocation"+\
       str(jobId)+" have been finished")
       logging.debug("Removing Allocation spec with ID : "+str(jobId))
       Allocation.remove(jobId)
       logging.debug("Removing allocation spec file for : "+str(allocation['id']))
       try:
           logging.debug("Spec file location is: "+str(allocation['allocation_spec_file']))
           os.remove(allocation['allocation_spec_file'])
       except Exception,ex:
           logging.debug("WARNING: "+str(ex))
           pass
       logging.debug("All jobs for this allocations have finished: contacting prodmgr")
       parameters={}
       parameters['jobSpecId']=str(jobId)
       parameters['events']=allocation['events_processed']
       parameters['request_id']=request_id=jobId.split('_')[1] 
       result=sendMessage(allocation['prod_mgr_url'],parameters)
       parameters['result']=result['result']
       newState=handleResult(parameters)
       Session.commit()
   else:
       logging.debug("Not all jobs for this allocation have finised. Not contacting prodmgr")
       Session.commit()

def sendMessage(url,parameters):
   try:
       logging.debug("Attempting to connect to server : "+url)
       finished=ProdMgrAPI.releaseJob(url,str(parameters['jobSpecId']),\
           int(parameters['events']),"ProdMgrInterface")
       # check if the associated allocation needs to be released.
       request_id=parameters['jobSpecId'].split('_')[1]
       return {'result':finished,'url':'fine'}
   except ProdAgentException, ex:
       logging.debug("Problem connecting to server: "+url+" "+str(ex))
       message={}
       message['server_url']=url
       message['type']='ReportJobSuccess'
       message['state']='reportJobSuccess'
       message['parameters']=parameters 
       storeMessage(message)
       return {'result':'start','url':'failed'}

def handleResult(parameters):
   global ms

   if parameters['result']=='start':
       return
   logging.debug("Handling result: "+str(parameters['result']))
   finished=int(parameters['result'])
   if finished==1:
       logging.debug("Request "+str(parameters['request_id'])+" is completed. Removing all allocations and request")
       logging.debug("Checking if we can emit RequestFinished event")
       if Workflow.isFinished(parameters['request_id']):
           logging.debug("Emitting RequestFinished event")
           ms.publish("RequestFinished",parameters['request_id'])
       Workflow.remove(parameters['request_id'])
   elif finished==2:
       logging.debug("Request "+str(parameters['request_id'])+" is not completed but allocation is")
       logging.debug("Emitting AllocationFinished event")
       ms.publish("AllocationFinished",parameters['request_id'])
       logging.debug("Checking if we need to submit a RequestFinished event for:"+str(parameters['request_id']))
       if Workflow.isDone(parameters['request_id']):
           if Workflow.isFinished(parameters['request_id']):
               ms.publish("RequestFinished",parameters['request_id'])
               logging.debug("Emitting RequestFinished event")
               Workflow.remove(parameters['request_id'])
   elif finished==0:
       logging.debug("Request "+str(parameters['request_id'])+" and allocation not completed")
   elif finished==3:
       logging.debug("Request "+str(parameters['request_id'])+" failed")
       logging.debug("Checking if we can emit RequestFailed event")
       if Workflow.isFinished(parameters['request_id']):
           ms.publish("RequestFinished",parameters['request_id'])
           logging.debug("Emitting RequestFinished event")
       Workflow.remove(parameters['request_id'])
   return "start"


def storeMessage(message):
   MessageQueue.insert("ProdMgrInterface",message['state'],message['server_url'],\
       message['type'],\
       message['parameters'],"00:00:10")
   logging.debug("Problem connecting to server "+message['server_url'])
   logging.debug("Attempt stored in message queue for later retries")
   Session.commit()






