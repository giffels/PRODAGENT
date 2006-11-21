#!/usr/bin/env python

import logging 
import os

from FwkJobRep.ReportParser import readJobReport
from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB import Session
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
       logging.debug('jobreport is: '+str(jobReport))
       logging.debug('jobspecid is: '+str(report[-1].jobSpecId))
       total = 0
       for fileinfo in report[-1].files:
           if  fileinfo['TotalEvents'] != None:
              total+=int(fileinfo['TotalEvents'])
       logging.debug('JobReport has been read processed: '+str(total)+' events')
       if stateParameters['jobType']=='failure':
           logging.debug("This job failed so we register 0 events")
           total=0
       request_id=report[-1].jobSpecId.split('_')[1] 
       prodMgrUrl=Job.getUrl(report[-1].jobSpecId)
       job_spec_location=Job.getLocation(report[-1].jobSpecId)
       Job.rm(report[-1].jobSpecId)
       os.remove(job_spec_location)

       parameters={}
       parameters['jobSpecId']=str(report[-1].jobSpecId)
       parameters['events']=total
       parameters['request_id']=request_id
       result=self.sendMessage(prodMgrUrl,parameters)
       parameters['result']=result['result']
       newState=self.handleResult(parameters)
       Session.commit()

   def sendMessage(self,url,parameters):
       try:
           logging.debug("Attempting to connect to server : "+url)
           finished=ProdMgrAPI.releaseJob(url,str(parameters['jobSpecId']),\
               int(parameters['events']),"ProdMgrInterface")
           # check if the associated allocation needs to be released.
           request_id=parameters['jobSpecId'].split('_')[1]
           return {'result':finished,'url':'fine'}
       except ProdAgentException, ex:
           logging.debug("Problem connecting to server: "+url+" "+str(ex))
           message={}
           message['server_url']=url
           message['type']='ReportJobSuccess'
           message['state']='reportJobSuccess'
           message['parameters']=parameters 
           self.storeMessage(message)
           return {'result':'start','url':'failed'}

   def handleResult(self,parameters):
       if parameters['result']=='start':
           return
       logging.debug("Handling result: "+str(parameters['result']))
       finished=int(parameters['result'])
       if finished==1:
           logging.debug("Request "+str(parameters['request_id'])+" is completed. Removing all allocations and request")
           Request.rm(parameters['request_id'])
       elif finished==2:
           logging.debug("Request "+str(parameters['request_id'])+" is not completed but allocation is")
       elif finished==0:
           logging.debug("Request "+str(parameters['request_id'])+" and allocation not completed")
       elif finished==3:
           logging.debug("Request "+str(parameters['request_id'])+" failed")
           Request.rm(parameters['request_id'])
       return "start"

registerHandler(ReportJobSuccess(),"ReportJobSuccess")







