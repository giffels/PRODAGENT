#!/usr/bin/env python
"""
_ProdMgrComponent_

Component that communicates with the ProdMgr to retrieve work
and report back details of completed jobs



"""


import os
import time

import logging
from logging.handlers import RotatingFileHandler

from MessageService.MessageService import MessageService

from ProdAgentDB import Session
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentCore.Codes import errors
from ProdMgrInterface import Allocation
from ProdMgrInterface import MessageQueue
from ProdMgrInterface import Job
from ProdMgrInterface import Request
from ProdMgrInterface import State

import ProdMgrInterface.Interface as ProdMgrAPI


class ProdMgrComponent:
    """
    _ProdMgrComponent_

    Component that interacts with the ProdMgr

    """
    def __init__(self, **args):

       try:
            self.args = {}
            self.args['Logfile'] = None
            self.args.update(args)
            if self.args['Logfile'] == None:
               self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                    "ComponentLog")
            #  //
            # // Log Handler is a rotating file that rolls over when the
            #//  file hits 1MB size, 3 most recent files are kept
            logHandler = RotatingFileHandler(self.args['Logfile'],
                                             "a", 1000000, 3)
            #  //
            # // Set up formatting for the logger and set the
            #//  logging level to info level
            logFormatter = logging.Formatter("%(asctime)s:%(module)s:%(message)s")
            logHandler.setFormatter(logFormatter)
            logging.getLogger().addHandler(logHandler)
            logging.getLogger().setLevel(logging.DEBUG)
            logging.info("ProdMgrComponent Started...")
       except Exception,ex:
            logging.debug("ERROR: "+str(ex))     
            raise

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)
        #  //
        # // Control Events for this component
        #//
        if event == "ProdMgrInterface:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return

        if event == "ProdMgrInterface:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        if event == "ProdMgrInterface:AddRequest":
            self.addRequest(payload)
            return

        #  //
        # // Actual work done by this component
        #//
        if event == "ResourcesAvailable":
            try:
                payloadVal = int(payload)
            except ValueError:
                payloadVal = 1
            self.retrieveWork(payloadVal)
            return

        if event == "JobSuccess":
            self.reportJobSuccess(payload)
            return

        if event == "GeneralJobFailure":
            self.reportJobFailure(payload)
            return



    def retrieveWork(self, numberOfJobs):
        """
        _retrieveWork_

        
        """
        # the algorithms works as follows:
        # check the last logged state. If it is start, check if there is anything 
        # left in the queue that failed when interacting with another application (prodmgr)
        # if not, proceed from the beginning, else proceed from the state that has
        # been logged.

        # we have a session for default 
        # and one for the states (called "ProdMgrInterface"), as we commit in between
        # logging calls and setting the explicit state

        # retrieve state (from database)
        # the state determines where we need to start.

        Session.connect("ProdMgrInterface")
        Session.set_current("ProdMgrInterface")

        # if this is the first time create the start state
        componentStateInfo=State.get("ProdMgrInterface")      
        if componentStateInfo=={}:
             logging.debug("ProdMgrInterface state creation")
             State.insert("ProdMgrInterface","start",{'requestIndex':0})
             componentState="start"
             requestIndex=0
        else:
             componentState=componentStateInfo['state']
             if componentStateInfo['parameters'].has_key('requestIndex'):
                 requestIndex=componentStateInfo['parameters']['requestIndex']
             else: 
                 requestIndex=0
        logging.debug("ProdMgrInterface state is: "+str(componentState))
        recover=False

        if componentState!='start':
            recover=True
        else:
            componentState="startAllocation"

        Session.commit()
         
        try:
            logging.debug("Retrieve work for "+str(numberOfJobs)+" jobs")
            logging.debug("There are "+str(Request.size())+" requests in the queue ")
            Session.commit()
           
            if componentState in ["startAllocation","acquireAllocation","acquireJob","downloadJobSpec"]:
                  # use two persistent queues for accounting in case
                  # of a crash.
               while (Allocation.size("messageLevel")<int(numberOfJobs)):
                  Session.commit()
                  # get request with highest priority:
                  # check if the prodmgr url of this request is not on the blacklist:
                  if componentState=="startAllocation":
                     # get request with highest priority:
                     request=Request.getHighestPriority(requestIndex)
                     # if this url is on the black list look for next one
                     while MessageQueue.hasURL(request['url']) and\
                          Request.size()<(requestIndex+1):
                          requestIndex+1 
                          request=Request.getHighestPriority(requestIndex)
                          State.setParameters("ProdMgrInterface",{'requestIndex':requestIndex})
                     # check if there are requests left:
                     if (Request.size())<(requestIndex+1):
                         # reset the index for next time
                         State.setParameters("ProdMgrInterface",{'requestIndex':0})
                         Session.commit()
                         logging.debug("We have no more requests in our queue for allocations"+\
                         " and jobs, bailing out")
                         break

                     # set parameters and state and commit for next session
                     parameters={'RequestID':request['id'],'ProdMgrURL':request['url'],'requestIndex':requestIndex}
                     State.setParameters("ProdMgrInterface",parameters)
                     componentState="acquireAllocation"
                     State.setState("ProdMgrInterface",componentState)
                     Session.commit() 
                     logging.debug("ProdMgrInterface state set to: acquireAllocation")


                  # part of these states will make calls to another server so we have a try except in 
                  # case this fails after which we need to take appropiate action. 
                  try:
                   if componentState=="acquireAllocation":
                      stateParameters=State.get("ProdMgrInterface")['parameters']
                      # check how many allocations we have from this request:
                      logging.debug("Checking allocations for request "+str(stateParameters['RequestID']))
                      idleRequestAllocations=Allocation.get('prodagentLevel','idle',str(stateParameters['RequestID']))
                      if ( ( (len(idleRequestAllocations)+Allocation.size("messageLevel")) )<numberOfJobs):
                          logging.debug("Not enough idle allocations for request "+stateParameters['RequestID']+\
                              ", will acquire more")
                          logging.debug("Contacting: "+stateParameters['ProdMgrURL']+" with payload "+\
                              stateParameters['RequestID']+','+str(int(numberOfJobs)-len(idleRequestAllocations)-Allocation.size("messageLevel")))
                          # only request the extra allocations if we have spare idle ones.
                          # the return format is an array of allocations ids we might get 
                          # less allocations back then we asked for, if there are not 
                          # enought available.

                          # if we crashed we might not have retrieved the last call.
                          if recover:
                              try:
                                  allocations=ProdMgrAPI.retrieve(stateParameters['ProdMgrURL'],"acquireAllocation","ProdMgrInterface")
                              except ProdAgentException,ex:
                                  if ex['ErrorNr']==3000:
                                      logging.debug("No uncommited service calls: "+str(ex))
                                      allocations=ProdMgrAPI.acquireAllocation(stateParameters['ProdMgrURL'],\
                                      stateParameters['RequestID'],\
                                      int(numberOfJobs)-len(idleRequestAllocations)-Allocation.size("messageLevel"),\
                                      "ProdMgrInterface")
                              except Exception,ex:
                                  # there is a problem connecting wrap up all relevant information and put
                                  # it in a queue for later insepection
                                  raise
                          else:
                              try:
                                  allocations=ProdMgrAPI.acquireAllocation(stateParameters['ProdMgrURL'],\
                                      stateParameters['RequestID'],\
                                      int(numberOfJobs)-len(idleRequestAllocations)-Allocation.size("messageLevel"),\
                                      "ProdMgrInterface")
                              except Exception,ex:
                                  # there is a problem connecting wrap up all relevant information and put
                                  # it in a queue for later insepection
                                  Session.rollback()
                                  MessageQueue.insert("ProdMgrInterface","retrieveWork",stateParameters['ProdMgrURL'],"acquireAllocation",stateParameters)
                                  Session.commit()
                                  raise
                          recover=False

                          # check if we got allocations back
                          if type(allocations)==bool:
                              if not allocations:
                          #       Request.rm([stateParameters['RequestID']])
                          #       Allocation.rm('messageLevel',stateParameters['RequestID']) 
                                 # request has finished?
                                 i='do_nothing'
                          else: 
                              logging.debug("Acquired allocations: "+str(allocations)) 
                              # we acquired allocations, update our own accounting:
                              Allocation.insert("prodagentLevel",allocations,stateParameters['RequestID'])
                              idleRequestAllocations=Allocation.get('prodagentLevel','idle',(stateParameters['RequestID']))
                              Allocation.insert("messageLevel",idleRequestAllocations,stateParameters['RequestID'])
                              Allocation.insert("requestLevel",idleRequestAllocations,stateParameters['RequestID'])
                              # we can commit as we made everything persistent at the client side.
                          ProdMgrAPI.commit()
                      else:
                          diff=numberOfJobs-len(Allocation.size("WorkfAllocation"))
                          Allocation.insert("messageLevel",idleRequestAllocations[0:diff])
                          Allocation.insert("requestLevel",idleRequestAllocations[0:diff])
                          logging.debug("Sufficient allocations acquired, proceeding with acquiring jobs")

                      componentState="acquireJob"
                      State.setState("ProdMgrInterface",componentState)
                      Session.commit() 
                      logging.debug("ProdMgrInterface state set to: acquireJob")
   
                   if(componentState in ["acquireJob","downloadJobSpec","jobSubmission"]):
   
                      # only acquire jobs if there are more than 0 allocations:
                      if (Allocation.size("requestLevel"))>0:
                          logging.debug("More than 1 allocation for which we need to retrieve a job") 

                          if componentState=="acquireJob":
                              stateParameters=State.get("ProdMgrInterface")['parameters']
                              request_id=Allocation.getRequest('requestLevel')
                              parameters={'numberOfJobs':Allocation.size('requestLevel'),
                                  'prefix':'job'}   
                              requestURL=Request.getUrl(request_id)
                              if recover:
                                  try:
                                      jobs=ProdMgrAPI.retrieve(requestURL,"acquireJob","ProdMgrInterface")
                                  except ProdAgentException,ex:
                                      if ex['ErrorNr']==3000:
                                          logging.debug("No uncommited service calls: "+str(ex))
                                          jobs=ProdMgrAPI.acquireJob(requestURL,request_id,parameters)
                                  except Exception,ex:
                                  # there is a problem connecting wrap up all relevant information and put
                                  # it in a queue for later insepection
                                      raise
                              else:
                                  try:
                                      jobs=ProdMgrAPI.acquireJob(requestURL,request_id,parameters)
                                  except Exception,ex:
                                  # there is a problem connecting wrap up all relevant information and put
                                  # it in a queue for later insepection
                                      # NOTE: move the allocations associated from message and request level to 
                                      # NOTE: something else
                                      Session.rollback()
                                      MessageQueue.insert("ProdMgrInterface","retrieveWork",requestURL,"acquireJob",stateParameters)
                                      Session.commit()
                                      raise
                              recover=False
                              Job.insert('requestLevel',jobs,request_id)
                              # we can now savely remove the request level allocation queue 
                              Allocation.rm('requestLevel')
                              ProdMgrAPI.commit()
                              logging.debug("Acquired the following jobs: "+str(jobs)) 
                              # we have the jobs, lets download there job specs and if finished emit
                              # a new job event. 
                              componentState="downloadJobSpec"
                              State.setState("ProdMgrInterface",componentState)
                              Session.commit() 
                              logging.debug("ProdMgrInterface state set to: "+componentState)

                          if componentState in ["downloadJobSpec","jobSubmission"]:
                              stateParameters=State.get("ProdMgrInterface")['parameters']
                              logging.debug("Downloading files to : "+self.args['JobSpecDir'])
                              jobs=Job.get('requestLevel')
                              for job in jobs:
                                  if componentState=="downloadJobSpec":
                                      stateParameters=State.get("ProdMgrInterface")['parameters']
                                      # we ignore the recover attribute as we check if something is downloaded
                                      recover=False
                                      if not Job.isDownloaded('requestLevel',job['jobSpecId']):
                                          targetDir=self.args['JobSpecDir']+'/'+job['jobSpecId'].replace('/','_')
                                          try:
                                              os.makedirs(targetDir)
                                          except:
                                              pass
                                          targetFile=job['URL'].split('/')[-1]
                                          logging.debug("Downloading: "+str(job['URL']))
                                          try:
                                              ProdMgrAPI.retrieveFile(job['URL'],targetDir+'/'+targetFile)
                                          except Exception,ex:
                                              # there is a problem connecting wrap up all relevant information and put
                                              # it in a queue for later insepection
                                              # NOTE: move job from request level to something else
                                              Session.rollback()
                                              MessageQueue.insert("ProdMgrInterface","retrieveWork",requestURL,"downloadJobSpec",stateParameters)
                                              Session.commit()
                                              raise

                                          Job.downloaded('requestLevel',job['jobSpecId'])
                                          ProdMgrAPI.commit()
                                         
                                          componentState="jobSubmission"
                                          State.setState("ProdMgrInterface",componentState)
                                          stateParameters={'jobSpecId':job['jobSpecId'],'targetFile':targetDir+'/'+targetFile,'requestIndex':requestIndex}
                                          State.setParameters("ProdMgrInterface",stateParameters)
                                          Session.commit() 
                                          logging.debug("ProdMgrInterface state set to: "+componentState)

   
                                  if componentState=="jobSubmission":
                                       recover=False
                                       stateParameters=State.get("ProdMgrInterface")['parameters']
                                       # emit event and delete job from request level.
                                       # we do not keep track of a job queue (like with allocations), as 
                                       # jobs are managed by other (persistent) parts of the ProdAgent

                                       # we retrieve the allocation id of this job from its id:
                                       allocation_id=stateParameters['jobSpecId'].split('/')[1]+'/'+\
                                           stateParameters['jobSpecId'].split('/')[3]
                                       logging.debug("Activating allocation: "+allocation_id+" for job: "+\
                                           stateParameters['jobSpecId'])
                                       Allocation.setState('prodagentLevel',allocation_id,'active') 
                                       Job.rm('requestLevel',stateParameters['jobSpecId'])
                                       self.ms.publish("CreateJob",stateParameters['targetFile']) 
                                       # NOTE this commit needs to be done under the ProdMgrInterface session
                                       # NOTE: not the default session
                                       self.ms.commit()

                                       componentState="downloadJobSpec"
                                       State.setState("ProdMgrInterface",componentState)
                                       stateParameters={'requestIndex':requestIndex}
                                       State.setParameters("ProdMgrInterface",stateParameters)
                                       Session.commit() 
                                       logging.debug("ProdMgrInterface state set to: downloadJobSpec")

                      stateParameters=State.get("ProdMgrInterface")['parameters']
                      Job.rm('requestLevel') 
                      componentState="startAllocation"
                      requestIndex=stateParameters['requestIndex']+1
                      stateParameters={'requestIndex':requestIndex}
                      State.setState("ProdMgrInterface",componentState)
                      State.setParameters("ProdMgrInterface",stateParameters)
                      Session.commit() 
                      logging.debug("ProdMgrInterface state set to: "+componentState)
                      
                  except Exception,ex:
                      # if an error occured here we assume it is something with connecting to a prodmgr.
                      # the state has been recorded in the queue for later use
                      logging.debug("A problem occured, most likeley while connecting to the prodmgr: "+str(ex))
                      logging.debug("The state has been saved in a queue and appropiate action has been taken")
                      time.sleep(100)
                      pass

               componentState="cleanup"
               State.setState("ProdMgrInterface",componentState)
               stateParameters={}
               State.setParameters("ProdMgrInterface",stateParameters)
               logging.debug("ProdMgrInterface state set to: "+componentState)
               Session.commit() 

            if componentState=="cleanup":
               Job.rm('requestLevel') 
               Allocation.rm('requestLevel') 
               Allocation.rm('messageLevel') 
               componentState="start"
               State.setState("ProdMgrInterface",componentState)
               Session.commit() 
               logging.debug("ProdMgrInterface state set to: "+componentState)
               # set session back to default
               Session.set_current("default")
        except Exception,ex:
            # depending on the error we need to rollback things
            raise ProdAgentException(errors[3004]+' '+str(ex),3004)


    def addRequest(self, requestURL):
        """
        _addRequest_

        URL points to ProdMgr containing the requests, it should contain
        2 arguments:
        RequestID: The id of the request in the ProdMgr
        Priority: The priority value for that request

        Eg:
        https://cmsprodmgr.somehost.com?RequestID=1234&Priority=5
 
        """
        #  //
        # // Parse the URL. 
        #//
        try:
            components=requestURL.split('?')
            prodMgr=components[0]
            requestId=components[1].split("=")[1]
            priority=components[2].split("=")[1]
            logging.debug("Add request: "+requestId+" with priority "+priority+" for prodmgr: "+prodMgr)
            Request.insert(requestId,priority,prodMgr)
            logging.debug("Added request. There are now "+str(Request.size())+" requests in the queue ")
        except Exception,ex:
            logging.debug("ERROR "+str(ex))



    def reportJobSuccess(self, frameworkJobReport):
        """
        _reportJobSuccess_

        Read the report provided and report the details back to the
        ProdMgr
        
        """
        logging.debug("Reporting Job Success "+frameworkJobReport)

    def reportFailure(self, frameworkJobReport):
        """
        _reportJobFailure_

        Read the report provided and report the details back to the ProdMgr

        """
        logging.debug("Reporting Job Failure"+frameworkJobReport)
        
        
        
        

    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
        try:
            # create message service
            self.ms = MessageService()
    
            # register
            self.ms.registerAs("ProdMgrInterface")
    
            logging.debug("Subscribing to messages: ")
            # subscribe to messages
            self.ms.subscribeTo("ProdMgrInterface:StartDebug")
            self.ms.subscribeTo("ProdMgrInterface:EndDebug")
            self.ms.subscribeTo("ProdMgrInterface:AddRequest")
            self.ms.subscribeTo("ResourcesAvailable")
            self.ms.subscribeTo("JobSuccess")
            self.ms.subscribeTo("GeneralJobFailure")
            logging.debug("Subscription completed ")
            
            
            # wait for messages
            while True:
                # SESSION: the message service uses the current session
                Session.connect()
                Session.start_transaction()

                type, payload = self.ms.get()
                logging.debug("Message type: "+str(type)+", payload: "+str(payload))
                self.__call__(type, payload)
                # we want to commit after the call has been sucessfuly completed
                # as this message will tell us the state of the component when
                # it crashed.

                # SESSION: when the message service commits it uses the current session
                # SESSION: this enables us to events and committing them without
                # SESSION: committing the event that initiated the handler.
                logging.debug("Committing Event "+str(type))
                self.ms.commit()

                # SESSION: commit and close remaining sessions
                Session.commit_all()
                Session.close_all()
                logging.debug("Finished handling event of type "+str(type))
        except Exception,ex:
            logging.debug("ERROR: "+str(ex))     
            raise
