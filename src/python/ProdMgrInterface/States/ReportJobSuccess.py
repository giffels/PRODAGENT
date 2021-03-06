#!/usr/bin/env python

import logging 
import os

from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdAgentCore.Codes import errors
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
from ProdAgent.WorkflowEntities import Aux

import ProdMgrInterface.Interface as ProdMgrAPI

class ReportJobSuccess(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self,stateParameters={}):
        logging.debug("Executing state: ReportJobSuccess")
        # examine if the job is a failure or not and treat it appropiately
        if stateParameters['jobType'] == 'failure':
            job_spec_id=stateParameters['jobReport']
        else:
            jobReport=stateParameters['jobReport']
            # retrieve relevant information:
            report=readJobReport(jobReport)
            logging.debug('jobreport is: '+str(jobReport))
            try:
                logging.debug('jobspecid is: '+str(report[-1].jobSpecId))
            except Exception,ex:
                msg = """WARNING: Something might be wrong with the generated job report.
                  Unless this is a merge job or a non prodmgr job 
                  then you can ignore this message.  If it is a prodmgr process job check if 
                  it exists and if it is proper formatted.  ProdMgr
                  will ignore this job as it has not sufficient information
                  to handle this. It might be that this is prodmgr job in which 
                  case some residu information is left in the database. %s
                  """ %(str(ex))
                logging.debug(msg)
                return
            job_spec_id=report[-1].jobSpecId
        logging.debug('Retrieving job spec')
        try:
            Job.setState(job_spec_id,"finished")
            logging.debug('Retrieving job spec for : '+str(job_spec_id))
            we_job=Job.get(job_spec_id)
            if not we_job:
               raise
            logging.debug('Retrieved job spec for: '+str(job_spec_id))
        #AF except:
        except Exception, ex:
            logging.debug("Job: "+str(job_spec_id)+ \
                " has been removed when "+\
                " workflow was removed. Not doing anything (case 1)")
            logging.debug("Details: %s"%(str(ex)))
            return
        if we_job['owner'] != 'prodmgr':
            logging.debug("Job: "+str(job_spec_id)+ \
                " not initiated by prodmgr. owner is "+str(we_job['owner'])+
                "\n Not doing anything (case 2)")
            return 

        if stateParameters['jobType']=='failure':
            logging.debug("This job failed so we register 0 events")
            total = 0
        else:
            total = 0
            files=[]
            for fileinfo in report[-1].files:
                if  fileinfo['TotalEvents'] != None:
                    total+=int(fileinfo['TotalEvents'])
                    file={'lfn':fileinfo['LFN'],'events':fileinfo['TotalEvents']}
                    files.append(file)
            logging.debug("Registering associated generated files for this job")
            File.register(job_spec_id,files)

        logging.debug("Finding missed events")
        missingEvents = int(we_job['events_allocated']) - total
        if missingEvents < 0:
            missingEvents = 0
        allocation = Allocation.get(we_job['allocation_id'])
        logging.debug("Updating allocation details")
        logging.debug("Registering "+str(missingEvents)+" missed events to associated allocation")
        Allocation.setEventsMissedIncrement(we_job['allocation_id'], missingEvents)
        # remove the job spec file.
        logging.debug("removing job spec file from job: "+str(job_spec_id))
        request_id=Aux.split(job_spec_id)[1] 
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
            Job.setEventsProcessedIncrement(job_spec_id, 0)
            logging.debug("Handling job Failure")
            if not HandleJobSuccess.ms:
                HandleJobSuccess.ms = self.ms
                HandleJobSuccess.trigger = self.trigger
            logging.debug("Handling job success")
            HandleJobSuccess.handleJob(job_spec_id)
        else:
            logging.debug("Feedback to ProdMgr is set to delay")
            logging.debug("Not bypassing the merge sensor")
            return

registerHandler(ReportJobSuccess(),"ReportJobSuccess")







