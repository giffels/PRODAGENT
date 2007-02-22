#!/usr/bin/env python

import logging 
import os
import time

from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdCommon.Database import Session
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

       # START JOBCUTTING HERE
       logging.debug("Starting job cutting")
       if stateParameters['RequestType']=='event':
           logging.debug('Start event cut for '+str(stateParameters['jobSpecId']))
           jobcuts=cut(stateParameters['jobSpecId'],int(stateParameters['jobCutSize']))
       else:
           logging.debug('Start file cut')
           jobcuts=cutFile(stateParameters['targetFile'],stateParameters['RequestID'])
       for jobcut in jobcuts:
           logging.debug("Emitting <CreateJob> event with payload: "+\
               str(jobcut['spec']))
           self.ms.publish("CreateJob",jobcut['spec'])
       # END JOBCUTTING HERE

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

registerHandler(JobSubmission(),"JobSubmission")







