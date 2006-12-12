#!/usr/bin/env python

import logging 

from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB import Session
from ProdMgrInterface import Job
from ProdMgrInterface import Request
from ProdMgrInterface import State
from ProdMgrInterface.JobCutter import cut
from ProdMgrInterface.Registry import registerHandler
from ProdMgrInterface.States.StateInterface import StateInterface 
import ProdMgrInterface.Interface as ProdMgrAPI

class JobSubmission(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: JobSubmission")
       stateParameters=State.get("ProdMgrInterface")['parameters']

       # START JOBCUTTING HERE
       logging.debug("Starting job cutting")
       jobcuts=cut(stateParameters['targetFile'],int(stateParameters['jobCutSize']))
       for jobcut in jobcuts:
           logging.debug("Emitting <CreateJob> event with payload: "+\
               str(jobcut['spec']))
           self.ms.publish("CreateJob",jobcut['spec'])
       # END JOBCUTTING HERE

       stateParameters['jobIndex']+=1
       componentState="EvaluateJobs"
       State.setState("ProdMgrInterface",componentState)
       State.setParameters("ProdMgrInterface",stateParameters)
       # NOTE this commit needs to be done under the ProdMgrInterface session
       # NOTE: not the default session. If the messesage service uses the session
       # NOTE: object the self.ms.commit() statement will be obsolete as it will
       # NOTE: be encapsulated in the Session.commit() statement.
       self.ms.commit()
       Session.commit()
       return componentState

registerHandler(JobSubmission(),"JobSubmission")







