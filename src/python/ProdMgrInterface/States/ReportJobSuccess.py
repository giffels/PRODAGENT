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
from ProdMgrInterface.Registry import registerHandler
from ProdMgrInterface.Registry import retrieveHandler
from ProdMgrInterface.States.Aux import HandleJobSuccess
from ProdMgrInterface.States.StateInterface import StateInterface 
import ProdMgrInterface.Interface as ProdMgrAPI

class ReportJobSuccess(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self,stateParameters={}):
       logging.debug("Executing state: ReportJobSuccess")
       # examine if the job is a failure or not and treat it appropiately
       if stateParameters['jobType']=='failure':
           logging.debug("This job failed so we register 0 events")
           total=0
           logging.debug('Retrieving job spec')
           try:
               job_spec_id=stateParameters['jobReport']
               logging.debug('Retrieving job spec for : '+str(job_spec_id))
               we_job=Job.get(job_spec_id)
               if not we_job:
                  raise
               logging.debug('Retrieved job spec for: '+str(job_spec_id))
           except:
              logging.debug("Job: "+str(job_spec_id)+" not initiated by prodmgr. Not doing anything (case 1)")
              return
           if not we_job['allocation_id']:
              logging.debug("Job: "+str(job_spec_id)+" not initiated by prodmgr. Not doing anything (case 2)")
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
           try:
               job_spec_id=report[-1].jobSpecId
               we_job=Job.get(job_spec_id)
               if not we_job:
                  raise
           except Exception,ex:
              logging.debug("Job: "+str(job_spec_id)+" not initiated by prodmgr. Not doing anything (case 3)")
              return
           if not we_job['allocation_id']:
              logging.debug("Job: "+str(job_spec_id)+" not initiated by prodmgr. Not doing anything (case 4)")
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
       #call the trigger code to commence cleanup of the job.
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

       # remove the job spec file.
       logging.debug("removing job spec file from job: "+str(job_spec_id))
       request_id=job_spec_id.split('_')[1] 
       logging.debug("request id : "+str(request_id))
       job_spec_location=we_job['job_spec_file']
       logging.debug("Retrieved (and removing) job spec file: "+str(job_spec_location))
       try:
          os.remove(job_spec_location)
       except:
          pass

       logging.debug('JobReport has been read processed: '+str(total)+' events')

       # determine what needs to be done next
       if( (stateParameters['prodMgrFeedback']=='direct') and (stateParameters['jobType']!='failure')):
           logging.debug("Feedback to ProdMgr is set to direct and job is not a failure")
           logging.debug("Commencing with procedure locally, bypassing the merge sensor")
           if not File.ms:
               File.ms=self.ms
           # the next call might send messages to the prodmgr itself, similar to what the merge sensor would do.
           fileNames=[]
           for file in files:
              fileNames.append(file['lfn']) 
           File.merged(fileNames)
           return
       elif(stateParameters['jobType']=='failure'):    
           logging.debug("Job failed during processing no files have been generated. bypassing merge sensor")
           Job.setEventsProcessedIncrement(job_spec_id,0)
           logging.debug("Handling job Failure")
           if not HandleJobSuccess.ms:
               HandleJobSuccess.ms=self.ms
           logging.debug("Handling job success")
           HandleJobSuccess.handleJob(job_spec_id)
       else:
           logging.debug("Feedback to ProdMgr is set to delay")
           logging.debug("Not bypassing the merge sensor")
           return

registerHandler(ReportJobSuccess(),"ReportJobSuccess")







