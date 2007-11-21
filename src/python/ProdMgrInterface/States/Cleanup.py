#!/usr/bin/env python

import logging 

from ProdAgentCore.Codes import errors
from ProdCommon.Database import Session
from ProdMgrInterface import MessageQueue
from ProdMgrInterface import State
from ProdMgrInterface.Registry import registerHandler
from ProdMgrInterface.States.StateInterface import StateInterface 
import ProdMgrInterface.Interface as ProdMgrAPI


class Cleanup(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: Cleanup")
       componentState="start"
       componentStateParameters={}
       State.setState("ProdMgrInterface",componentState)
       State.setParameters("ProdMgrInterface",componentStateParameters)
       Session.commit()
       # set session back to default
       Session.set_session("default")
       return componentState

registerHandler(Cleanup(),"Cleanup")







