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


class EvaluateJobs(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: EvaluateJobs")
       stateParameters=State.get('ProdMgrInterface')['parameters']
       logging.debug(str(Job.size("requestLevel"))+" jobs in list. Index is: "+\
           str(stateParameters['jobIndex']))
       if (Job.size("requestLevel")-1)<stateParameters['jobIndex']:
           State.setState("ProdMgrInterface","EvaluateAllocations")
           stateParameters['requestIndex']+=1
           State.setParameters("ProdMgrInterface",stateParameters)
           Session.commit()
           return "EvaluateAllocations" 
       State.setState("ProdMgrInterface","DownloadJobSpec")
       Session.commit()
       return "DownloadJobSpec" 

registerHandler(EvaluateJobs(),"EvaluateJobs")







