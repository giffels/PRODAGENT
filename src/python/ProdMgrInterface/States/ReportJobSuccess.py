#!/usr/bin/env python

import logging 
import os

from FwkJobRep.ReportParser import readJobReport
from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdCommon.Database import Session
from ProdMgrInterface import MessageQueue
from ProdAgent.WorkflowEntities import Allocation
from ProdAgent.WorkflowEntities import File
from ProdAgent.WorkflowEntities import Job 
from ProdAgent.WorkflowEntities import Workflow
from ProdMgrInterface import State
from ProdMgrInterface.Registry import registerHandler
from ProdMgrInterface.States.StateInterface import StateInterface 
import ProdMgrInterface.Interface as ProdMgrAPI


class ReportJobSuccess(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: ReportJobSuccess")
       stateParameters=State.get("ProdMgrInterface")['parameters']
       if stateParameters['jobType']=='failure':
           logging.debug("This job failed so we register 0 events")
           total=0
           logging.debug('Retrieving job spec')
           job_spec_id=stateParameters['jobReport']
           logging.debug('Retrieved job spec')
           we_job=Job.get(job_spec_id)
           if not we_job['allocation_id']:
              logging.debug("Job: "+str(job_spec_id)+" not initiated by prodmgr. Not doing anything")
              return 
       else:
           jobReport=stateParameters['jobReport']
           # retrieve relevant information:
           report=readJobReport(jobReport)
           logging.debug('jobreport is: '+str(jobReport))
           try:
               logging.debug('jobspecid is: '+str(report[-1].jobSpecId))
           except Exception,ex:
               msg="""ERROR: Something is wrong with the generated job report.
                  check if it exists and if it is proper formatted. ProdMgr
                  will ignore this job as it has not sufficient information
                  to handle this. It might be that this is prodmgr job in which 
                  case some residu information is left in the database. """
               logging.debug(msg)
               return
           job_spec_id=report[-1].jobSpecId
           we_job=Job.get(job_spec_id)
           if not we_job['allocation_id']:
              logging.debug("Job: "+str(job_spec_id)+" not initiated by prodmgr. Not doing anything")
              return 
           total = 0
           files=[]
           for fileinfo in report[-1].files:
               if  fileinfo['TotalEvents'] != None:
                  total+=int(fileinfo['TotalEvents'])
                  file={'lfn':fileinfo['LFN'],'events':fileinfo['TotalEvents']}
                  files.append(file)
           logging.debug("Registering associated generated files for this job")
           File.register(job_spec_id,files)

       Job.setEventsProcessedIncrement(job_spec_id,total) 
       Job.remove(job_spec_id)
       #NOTE: here we call the trigger code.
       try:
          self.trigger.flagSet("cleanup",job_spec_id,"ProdMgrInterface")
          self.trigger.setFlag("cleanup", job_spec_id,"ProdMgrInterface")
       except Exception,ex:
          logging.debug("WARNING: problem with prodmgr flag setting\n"
              +str(ex)+"\n"
              +" it might be that this job was generated outside the prodmgr\n"
              +" If that is the case, do not panic")
          logging.debug("ProdMgr does nothing with this job")
          return

       logging.debug('JobReport has been read processed: '+str(total)+' events')

       # remove the job spec file.
       logging.debug("removing job spec file from job: "+str(job_spec_id))
       request_id=job_spec_id.split('_')[1] 
       logging.debug("request id : "+str(request_id))
       job_spec_location=we_job['job_spec_file']
       logging.debug("retrieved job spec location: "+str(job_spec_location))
       try:
          os.remove(job_spec_location)
       except:
          pass
       # update the events processed by this job
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
           result=self.sendMessage(allocation['prod_mgr_url'],parameters)
           parameters['result']=result['result']
           newState=self.handleResult(parameters)
           Session.commit()
       else:
           logging.debug("Not all jobs for this allocation have finised. Not contacting prodmgr")
           Session.commit()

   def sendMessage(self,url,parameters):
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
           self.storeMessage(message)
           return {'result':'start','url':'failed'}

   def handleResult(self,parameters):
       if parameters['result']=='start':
           return
       logging.debug("Handling result: "+str(parameters['result']))
       finished=int(parameters['result'])
       if finished==1:
           logging.debug("Request "+str(parameters['request_id'])+" is completed. Removing all allocations and request")
           logging.debug("Checking if we can emit RequestFinished event")
           if Workflow.isFinished(parameters['request_id']):
               logging.debug("Emitting RequestFinished event")
               self.ms.publish("RequestFinished",parameters['request_id'])
           Workflow.remove(parameters['request_id'])
       elif finished==2:
           logging.debug("Request "+str(parameters['request_id'])+" is not completed but allocation is")
           logging.debug("Emitting AllocationFinished event")
           self.ms.publish("AllocationFinished",parameters['request_id'])
           logging.debug("Checking if we need to submit a RequestFinished event for:"+str(parameters['request_id']))
           if Workflow.isDone(parameters['request_id']):
               if Workflow.isFinished(parameters['request_id']):
                   self.ms.publish("RequestFinished",parameters['request_id'])
                   logging.debug("Emitting RequestFinished event")
                   Workflow.remove(parameters['request_id'])
       elif finished==0:
           logging.debug("Request "+str(parameters['request_id'])+" and allocation not completed")
       elif finished==3:
           logging.debug("Request "+str(parameters['request_id'])+" failed")
           logging.debug("Checking if we can emit RequestFailed event")
           if Workflow.isFinished(parameters['request_id']):
               self.ms.publish("RequestFinished",parameters['request_id'])
               logging.debug("Emitting RequestFinished event")
           Workflow.remove(parameters['request_id'])
       return "start"

registerHandler(ReportJobSuccess(),"ReportJobSuccess")







