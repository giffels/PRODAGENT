#!/usr/bin/env python

import logging 
import os
import time

from ProdCommon.Database import Session

from ProdAgentCore.Codes import errors

from ProdMgrInterface import State
from ProdMgrInterface.JobCutter import cut
from ProdMgrInterface.JobCutter import cutFile
from ProdMgrInterface.Registry import registerHandler
from ProdMgrInterface.States.StateInterface import StateInterface 


class JobSubmission(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: JobSubmission")
       stateParameters=State.get("ProdMgrInterface")['parameters']

       try:
           os.makedirs(stateParameters['jobSpecDir'])
       except Exception, ex:
           logging.debug("WARNING: "+str(ex)+". Directory probably already exists")
           pass

       self.jobCutAccounting(stateParameters)

       stateParameters['jobIndex']+=1
       componentState="AcquireRequest"
       State.setState("ProdMgrInterface",componentState)
       State.setParameters("ProdMgrInterface",stateParameters)
       # NOTE this commit needs to be done under the ProdMgrInterface session
       # NOTE: not the default session. If the messesage service uses the session
       # NOTE: object the self.ms.commit() statement will be obsolete as it will
       # NOTE: be encapsulated in the Session.commit() statement.
       Session.commit()
       return componentState

   def jobCutAccounting(self,stateParameters, allocation = None):
       # START JOBCUTTING HERE
       logging.debug("Starting job cutting")
       if stateParameters['RequestType']=='event':
           logging.debug('Start event cut for '+str(stateParameters['jobSpecId']))
           jobcuts=cut(stateParameters['jobSpecId'],int(stateParameters['jobCutSize']), allocation = None)
       else:
           logging.debug('Start file cut')
           jobcuts=cutFile(stateParameters['jobSpecId'],stateParameters['jobCutSize'],stateParameters['maxJobs'])
       jobSpecs = []
       for jobcut in jobcuts['specs']:
           if self.args['JobInjection'] == 'direct':
               logging.debug("Emitting CreateJob event with payload: "+\
                   str(jobcut['spec']))
               self.ms.publish("CreateJob", jobcut['spec'])
           else:
           # get information required for bulkQueueJobs API
               jobData = {
                       'JobSpecId' :        jobcut['id'], \
                       'JobSpecFile' :      jobcut['spec'], \
                       'JobType' :          jobcut['job_type'], \
                       'WorkflowSpecId' :   jobcuts['workflow'], \
                       'WorkflowPriority' : jobcuts['priority']
                       }
               jobSpecs.append(jobData)
           # fill the JobQueue
       if self.args['JobInjection'] != 'direct':
            sites=[]
            logging.info("Sites List: %s" % sites)
            self.args['JobQueue'].loadSiteMatchData()
            self.args['JobQueue'].insertJobSpecsForSites(sites, *jobSpecs)
       # END JOBCUTTING HERE
       return jobcuts

registerHandler(JobSubmission(),"JobSubmission")







