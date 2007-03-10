#!/usr/bin/env python

import logging 
import os

from ProdMgrInterface import State
from ProdMgrInterface.Registry import registerHandler
from ProdMgrInterface.States.Aux import HandleJobSuccess
from ProdMgrInterface.States.StateInterface import StateInterface 


class ReportJobSuccessFinal(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self,stateParameters={}):
       logging.debug("Executing state: ProdMgrInterface:ReportJobSuccess")
       logging.debug("Retrieving event information for job "+str(stateParameters['id']))
       # we break here until we have the merge sensor part sorted out.
       if not HandleJobSuccess.ms:
           HandleJobSuccess.ms=self.ms
       logging.debug("Handling job success")
       HandleJobSuccess.handleJob(stateParameters['id'])

registerHandler(ReportJobSuccessFinal(),"ReportJobSuccessFinal")







