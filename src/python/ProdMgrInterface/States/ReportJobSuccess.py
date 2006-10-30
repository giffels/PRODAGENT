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


class ReportJobSuccess(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: ReportJobSuccess")
       stateParameters=State.get("ProdMgrInterface")['parameters']
       jobReport=stateParameters['jobReport']
       # retrieve relevant information:
       report=readJobReport(jobReport)
       logging.debug('jobspecid is: '+str(report[-1].jobSpecId))
       total = 0
       for fileinfo in report[-1].files:
           if  fileinfo['TotalEvents'] != None:
              total+=int(fileinfo['TotalEvents'])
       logging.debug('JobReport has been read processed: '+str(total)+' events')
       request_id=report[-1].jobSpecId.split('/')[1] 
       prodMgrUrl=Request.getUrl(request_id)
       logging.debug('Attempting to contact: '+prodMgrUrl)

       # for this queue it does not matter in what order they are delivered.
       try:
           finished=ProdMgrAPI.releaseJob(prodMgrUrl,str(report[-1].jobSpecId),total,"ProdMgrInterface")
       except Exception,ex:
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

       if finished:
           # if the request is finished, remove it from our queue
           # NOTE: perhaps we should send a kill event for remaining jobs?
           request_id=message['parameters']['jobSpecId'].split('/')[1]
           Request.rm(request_id)
           Allocation.rm(request_id)
       allocation_id=report[-1].jobSpecId.split('/')[1]+'/'+\
       report[-1].jobSpecId.split('/')[2]+'/'+\
       report[-1].jobSpecId.split('/')[3]
       Allocation.setState('prodagentLevel',allocation_id,'idle')
       Session.commit()
       # set session back to default
       Session.set_current("default")
       return "start"



registerHandler(ReportJobSuccess(),"ReportJobSuccess")







