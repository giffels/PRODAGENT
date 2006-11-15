#!/usr/bin/env python

import logging 

from FwkJobRep.ReportParser import readJobReport
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


class ReportJobFailure(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: ReportJobFailure")
       stateParameters=State.get("ProdMgrInterface")['parameters']
       jobReport=stateParameters['jobReport']
       # retrieve relevant information:
       report=readJobReport(jobReport)
       request_id=report[-1].jobSpecId.split('_')[1]
       prodMgrUrl=Request.getUrl(request_id)
       logging.debug('Attempting to contact: '+prodMgrUrl)
       # set session back to default
       Session.set_current("default")
       try:
           finished=ProdMgrAPI.releaseJob(prodMgrUrl,str(report[-1].jobSpecId),0,"ProdMgrInterface")
       except Exception,ex:
           #NOTE: we need to handle different exceptions
           # there is a problem connecting wrap up all relevant information and put
           # it in a queue for later inspection
           stateParameters['jobSpecId']=str(report[-1].jobSpecId)
           stateParameters['events']=total
           MessageQueue.insert("ProdMgrInterface","reportJobSuccess",prodMgrUrl,"ReportJobSuccess",stateParameters,"00:00:10")
           logging.debug("Problem connecting to server "+prodMgrUrl+" : "+str(ex))
           logging.debug("Attempt stored in job queue for later retries")
           Session.commit()
           Session.set_current("default")
           return 'start'
       if finished==1 or finished==3:
           # if the request is finished, remove it from our queue
           # NOTE: perhaps we should send a kill event for remaining jobs?
           request_id=message['parameters']['jobSpecId'].split('_')[1]
           Request.rm(request_id)
           Allocation.rm(request_id)
       elif finished==0:
           allocation_id=report[-1].jobSpecId.split('_')[1]+'/'+\
           report[-1].jobSpecId.split('_')[3]
           Allocation.setState('prodagentLevel',allocation_id,'idle')
       Session.commit()
       # set session back to default
       Session.set_current("default")
       return "start"

registerHandler(ReportJobFailure(),"ReportJobFailure")















