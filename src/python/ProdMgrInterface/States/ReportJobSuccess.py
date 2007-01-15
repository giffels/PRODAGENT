#!/usr/bin/env python

import logging 
import os

from FwkJobRep.ReportParser import readJobReport
from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB import Session
from ProdMgrInterface import MessageQueue
from ProdMgrInterface import Job
from ProdMgrInterface import JobCut
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

       # remove the job cut spec file.
       logging.debug("removing job cut spec file")
       request_id=report[-1].jobSpecId.split('_')[1] 
       job_spec_location=JobCut.getLocation(report[-1].jobSpecId)
       try:
          os.remove(job_spec_location)
       except:
          pass
       # update the events processed by this jobcut
       JobCut.eventsProcessed(report[-1].jobSpecId,total)

       logging.debug("Evaluate job associated to job cut "+str(report[-1].jobSpecId))
       jobId=Job.id(report[-1].jobSpecId)
       logging.debug("Associated job is: "+str(jobId)) 
       if Job.jobCutsFinished(jobId):
         # retrieve the number of processed events
           events=JobCut.events(jobId)
         # remove all entries associated to job in job_cuts
           JobCut.rm(jobId)
         # remove the job information and files
           request_id=jobId.split('_')[1] 
           prodMgrUrl=Job.getUrl(jobId)
           job_spec_location=Job.getLocation(jobId)
           logging.debug('Removing Job with id: '+jobId)
           Job.rm(jobId)
           try:
              os.remove(job_spec_location)
           except:
              pass
           logging.debug("All cuts have finished, contacting prodmgr")
         # send a message to prodmgr on the status of the allocated job.
           parameters={}
           parameters['jobSpecId']=str(jobId)
           parameters['events']=events
           parameters['request_id']=request_id
           result=self.sendMessage(prodMgrUrl,parameters)
           parameters['result']=result['result']
           newState=self.handleResult(parameters)
           Session.commit()
       else:
           logging.debug("Not all job cuts for this job have finised. Not contacting prodmgr")
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
           self.ms.publish("RequestFinished",parameters['request_id'])
           logging.debug("Emitting RequestFinished event")
           Request.rm(parameters['request_id'])
       elif finished==2:
           logging.debug("Request "+str(parameters['request_id'])+" is not completed but allocation is")
           logging.debug("Emitting AllocationFinished event")
           self.ms.publish("AllocationFinished",parameters['request_id'])
       elif finished==0:
           logging.debug("Request "+str(parameters['request_id'])+" and allocation not completed")
       elif finished==3:
           logging.debug("Request "+str(parameters['request_id'])+" failed")
           Request.rm(parameters['request_id'])
       return "start"

registerHandler(ReportJobSuccess(),"ReportJobSuccess")







