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


class Cleanup(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: Cleanup")
       Job.rm('requestLevel')
       Allocation.rm('requestLevel')
       Allocation.rm('messageLevel')
       componentState="start"
       componentStateParameters={}
       State.setState("ProdMgrInterface",componentState)
       State.setParameters("ProdMgrInterface",componentStateParameters)
       Session.commit()
       # set session back to default
       Session.set_current("default")
       return componentState

registerHandler(Cleanup(),"Cleanup")







