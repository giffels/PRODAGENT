
#!/usr/bin/env python

import logging 
import os

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


class DownloadJobSpec(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: DownloadJobSpec")
       stateParameters=State.get("ProdMgrInterface")['parameters']
       job=Job.get('requestLevel')[stateParameters['jobIndex']]
       targetDir=stateParameters['jobSpecDir']+'/'+job['jobSpecId'].replace('/','_')
       targetFile=job['URL'].split('/')[-1]
       logging.debug("targetFile for download is: "+targetFile)

       # if it is queue, restore the message and request level
       # allocations
       if stateParameters['stateType']=='queue':
           Allocation.mv('requestLevelQueued','requestLevel',request_id)
           Allocation.mv('messageLevelQueued','messageLevel',request_id)
           Session.commit()
           # now everything is back to normal, we do not have to recover
           # as making the call is what went wrong.
           stateParameters['stateType']=='normal'


       if not Job.isDownloaded('requestLevel',job['jobSpecId']):
           logging.debug("Downloading specification file to: "+str(targetDir))
           try:
               os.makedirs(targetDir)
           except Exception, ex:
               logging.debug("WARNING: "+str(ex))
               pass
           logging.debug(" Downloading: "+str(job['URL']))
           try:
               ProdMgrAPI.retrieveFile(job['URL'],targetDir+'/'+targetFile)
           except Exception,ex:
               # there is a problem connecting wrap up all relevant information and put
               # it in a queue for later insepection
               # NOTE: move job from request level to something else
               Session.rollback()
               request_id=job['jobSpecId'].split('_')[1]
               Job.mv('requestLevel','requestLevelQueued',request_id)
               Allocation.mv('messageLevel','messageLevelQueued',request_id)
               stateParameters['requestIndex']=-1
               MessageQueue.insert("ProdMgrInterface","retrieveWork",requestURL,"DownloadJobSpec",stateParameters)
               componentState="AcquireRequest"
               State.setState("ProdMgrInterface",componentState)
               Session.commit()
               logging.debug("Problem connecting to server "+stateParameters['ProdMgrURL']+" : "+str(ex))
               return componentState

           Job.downloaded('requestLevel',job['jobSpecId'])
           ProdMgrAPI.commit()

       componentState="JobSubmission"
       State.setState("ProdMgrInterface",componentState)
       stateParameters['jobSpecId']=job['jobSpecId']
       stateParameters['targetFile']=targetDir+'/'+targetFile
       logging.debug("Absolute targetFile = "+stateParameters['targetFile'])
       State.setParameters("ProdMgrInterface",stateParameters)
       Session.commit()
       return componentState

registerHandler(DownloadJobSpec(),"DownloadJobSpec")







