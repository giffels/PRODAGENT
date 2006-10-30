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


class JobSubmission(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: JobSubmission")
       stateParameters=State.get("ProdMgrInterface")['parameters']
       # emit event and delete job from request level.
       # we do not keep track of a job queue (like with allocations), as
       # jobs are managed by other (persistent) parts of the ProdAgent

       # we retrieve the allocation id of this job from its id:
       allocation_id=stateParameters['jobSpecId'].split('/')[1]+'/'+\
           stateParameters['jobSpecId'].split('/')[2]+'/'+\
           stateParameters['jobSpecId'].split('/')[3]
       logging.debug("Activating allocation: "+allocation_id+" for job: "+\
           stateParameters['jobSpecId'])
       logging.debug('test: prodagentLevel '+allocation_id+' active')
       Allocation.setState('prodagentLevel',allocation_id,'active')
       Job.rm('requestLevel',stateParameters['jobSpecId'])

       self.ms.publish("CreateJob",stateParameters['targetFile'])

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







