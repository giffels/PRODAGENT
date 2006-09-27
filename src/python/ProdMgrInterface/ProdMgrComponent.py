#!/usr/bin/env python
"""
_ProdMgrComponent_

Component that communicates with the ProdMgr to retrieve work
and report back details of completed jobs



"""


import os
import time
from MessageService.MessageService import MessageService

import logging
from logging.handlers import RotatingFileHandler

from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdMgrInterface.PriorityQueue import PriorityQueue
from ProdMgrInterface.AllocationQueue import AllocationQueue
from ProdMgrInterface.JobQueue import JobQueue
import ProdMgrInterface.Interface as ProdMgrAPI

from ProdAgentDB.Session import *


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
    
            # priorities hold requests while the 
            # job queue holds job information 
            self.priorityQueue          = PriorityQueue()
            self.allocationQueue        = AllocationQueue()
            self.jobQueue               = JobQueue()
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
        #  //
        # // Talk to the ProdMgr:
        #//
        # 1. Is work available  for the top request
        #    No => continue try other request.
        #    Yes => Get work and break this loop if we have the maximum number of jobs
        #           Then publish CreateJob for all job specs

        # our first approximation will not deal with failure modes. Once we have
        # this working we added more failure handling to it. 

        # we do not know if we recovered from a failure, thus we check to see
        # the last call we made that has not been commited. If there is none, it means
        # that we can proceed from the start. If there is one it describes the state
        # we where at the time of the crash.

        #NOTE: persistency actions part of the global schema should be committed 
        #NOTE: immetiately, while persitency actions part of the local schema
        #NOTE: should be committed at the end of handling this.
        

        #retrieve state from the database, if not start take appropiate action.
        #NOTE: the states should be made persistent in the database.

        # keep track of a black list. If we have problem connecting with a server
        # we avoid using it again during handling this event.
        #NOTE: we might make this persitent so we can use coolof periods
        prodMgrBlackList={}

        # we have a session for default (set by the message service)
        # and one for the states, as we commit in between
        # logging calls and setting the explicit state can create
        # and close their own connections not using a session.

        # we use our own session instead of the default set by the message service.

        # retrieve state (from database)
        # the state determines where we need to start.
        componentState="start"
        stateParameters={'requestIndex':0}
 
        logging.debug("ProdMgrInterface state set to: "+str(componentState))

        recover=False
        if componentState!='start':
            recover=True
         

        try:
            logging.debug("Retrieve work for "+str(numberOfJobs)+" jobs")
            logging.debug("There are "+str(len(self.priorityQueue))+" requests in the queue ")
           
            # a mapping we use later on for job allocations
            # so we do not have to query the database twice.
            requestIndex=stateParameters['requestIndex']
    
            if componentState in ["start","acquireAllocation","acquireJob","downloadJobSpec"]:

               if componentState=="start":
                  # use two persistent queues for accounting in case
                  # of a crash.
                  self.workAllocationQueue    = AllocationQueue()
 
               while (len(self.workAllocationQueue)<int(numberOfJobs)):
                  self.sessionAllocationQueue = AllocationQueue()
                  self.sessionJobQueue        = JobQueue()
                  # get request with highest priority:
                  # check if the request is not on the blacklist:

                  if componentState=="start":
                     # order requests as we want to take higher priorities first.
                     self.priorityQueue.orderRequests()

                     # get request with highest priority:
                     request=self.priorityQueue[requestIndex]

  
                     while prodMgrBlackList.has_key(request['ProdMgrURL']) and\
                          (len(self.priorityQueue)<(requestIndex+1)):
                          request=self.priorityQueue[requestIndex]
                          requestIndex+=1
                     # check if there are requests left:
                     if (len(self.priorityQueue)<(requestIndex+1)):
                          break
   
                     logging.debug("ProdMgrInterface state set to: acquireAllocation")
                     componentState="acquireAllocation"
                     stateParameters={'RequestID':request['RequestID'],'ProdMgrURL':request['ProdMgrURL'],'requestIndex':requestIndex}

                  # do we have enough total allocations?
                  if (len(self.priorityQueue)<(requestIndex+1)):
                      logging.debug("We have no more requests in our queue for allocations"+\
                         " and jobs, bailing out")
                      break

                  try:
   
                   if componentState=="acquireAllocation":
   
                      # check how many allocations we have from this request:
                      logging.debug("Checking allocations for request "+str(stateParameters['RequestID']))
                      idleRequestAllocations=self.allocationQueue.getIdle(stateParameters['RequestID'])
                      if ( (len(idleRequestAllocations)+len(self.workAllocationQueue) )<numberOfJobs):
                          logging.debug("Not enough idle allocations request "+request['RequestID']+\
                              ", will acquire more")
                          logging.debug("Contacting: "+stateParameters['ProdMgrURL']+" with payload "+\
                              stateParameters['RequestID']+','+str(int(numberOfJobs)-len(idleRequestAllocations)-len(self.sessionAllocationQueue)))
                          # only request the extra allocations if we have spare idle ones.
                          # the return format is an array of allocations ids
                          # we might get less allocations back then we asked for, if there are not 
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
                                      int(numberOfJobs)-len(idleRequestAllocations)-len(self.workAllocationQueue),\
                                      "ProdMgrInterface")
                              except Exception,ex:
                                  raise
                          else:
                              allocations=ProdMgrAPI.acquireAllocation(stateParameters['ProdMgrURL'],\
                                  stateParameters['RequestID'],\
                                  int(numberOfJobs)-len(idleRequestAllocations)-len(self.workAllocationQueue),\
                                  "ProdMgrInterface")
                          recover=False

                          # check if we got allocations back
                          if type(allocations)==bool:
                              if not allocations:
                                 self.priorityQueue.delRequest(stateParameters['RequestID']) 
                                 self.workAllocationQueue.delAllocations(stateParameters['RequestID'])
                                 # request has finished
                          else: 
                              logging.debug("Acquired allocations: "+str(allocations)) 
                              # we acquired allocations, update our own accounting:
                              self.allocationQueue.add(allocations)
                              idleRequestAllocations=self.allocationQueue.getIdle(stateParameters['RequestID'])
                              self.workAllocationQueue.add(idleRequestAllocations)
                              self.sessionAllocationQueue.add(idleRequestAllocations) 
                               # we can commit as we made everything persistent at the client side.
                          ProdMgrAPI.commit()
                      else:
                          diff=numberOfJobs-len(self.workAllocationQueue)
                          self.sessionAllocationQueue.add(idleRequestAllocations[0:diff])
                          self.workAllocationQueue.add(idleRequestAllocations[0:diff])
                          logging.debug("Sufficient allocations acquired, proceeding with acquiring jobs")

                      logging.debug("ProdMgrInterface state set to: acquireJob")
                      componentState="acquireJob"
                      stateParameters={'requestIndex':requestIndex}
   
                   if(componentState in ["acquireJob","downloadJobSpec","jobSubmission"]):
   
                      # only acquire jobs if there are more than 0 allocations:
                      if (len(self.sessionAllocationQueue)>0):
    
                          if componentState=="acquireJob":
                              part=self.sessionAllocationQueue[0]['AllocationID'].split('/')
                              # part 0 is the request id
                              request_id=part[0]
                              parameters={'numberOfJobs':len(self.sessionAllocationQueue),
                                  'prefix':'job'}   
                              requestURL=self.priorityQueue.retrieveRequest(request_id)['ProdMgrURL']
                              
                              if recover:
                                  try:
                                      jobs=ProdMgrAPI.retrieve(requestURL,"acquireJob","ProdMgrInterface")
                                  except ProdAgentException,ex:
                                      if ex['ErrorNr']==3000:
                                          logging.debug("No uncommited service calls: "+str(ex))
                                          jobs=ProdMgrAPI.acquireJob(requestURL,request_id,parameters)
                                  except Exception,ex:
                                      raise
                              else:
                                  jobs=ProdMgrAPI.acquireJob(requestURL,request_id,parameters)
                              recover=False

                              self.sessionJobQueue.add(jobs)
                              self.jobQueue.add(jobs)
                              # we can now savely remove the sessionAllocationQueue
                              del self.sessionAllocationQueue
                              ProdMgrAPI.commit()
                              logging.debug("Acquired the following jobs: "+str(self.sessionJobQueue)) 
                              # we have the jobs, lets download there job specs and if finished emit
                              # a new job event. 

                              logging.debug("ProdMgrInterface state set to: downloadJobSpec")
                              componentState="downloadJobSpec"
                              stateParameters={'requestIndex':requestIndex}

                          if componentState in ["downloadJobSpec","jobSubmission"]:
   
                              logging.debug("Downloading files to : "+self.args['JobSpecDir'])
                              for job in self.sessionJobQueue:
                                  if componentState=="downloadJobSpec":
                                      recover=False
                                      if not self.sessionJobQueue.isDownloaded(job['JobSpecID']):
                                          targetDir=self.args['JobSpecDir']+'/'+job['JobSpecID'].replace('/','_')
                                          try:
                                              os.makedirs(targetDir)
                                          except:
                                              pass
                                          targetFile=job['JobSpecURL'].split('/')[-1]
                                          logging.debug("Downloading: "+str(job['JobSpecURL']))
                                          try:
                                              ProdMgrAPI.retrieveFile(job['JobSpecURL'],targetDir+'/'+targetFile)
                                          except Exception,ex:
                                              raise

                                          self.sessionJobQueue.downloaded(job['JobSpecID'])
                                          ProdMgrAPI.commit()
                                         

                                          logging.debug("ProdMgrInterface state set to: emit JobSubmission events")
                                          componentState="jobSubmission"
                                          stateParameters={'JobSpecID':job['JobSpecID'],'targetFile':targetDir+'/'+targetFile,'requestIndex':requestIndex}
   
                                  if componentState=="jobSubmission":
                                       recover=False

                                       # emit event and delete job from session entry.
                                       # we do not keep track of a job queue (like with allocations), as 
                                       # jobs are managed by other (persistent) parts of the ProdAgent
                                       #NOTE: we also need to update the allocations and set the one
                                       #NOTE: associated to this job to active
                                       self.sessionJobQueue.delJob(stateParameters['JobSpecID'])
                                       self.ms.publish("CreateJob",stateParameters['targetFile']) 
                                       self.jobQueue.delJob(stateParameters['JobSpecID'])
                                       logging.debug("ProdMgrInterface state set to: downloadJobSpec")
                                       componentState="downloadJobSpec"
                                       stateParameters={'requestIndex':requestIndex}

                              
                              logging.debug("ProdMgrInterface state set to: start")
                              componentState="start"
                              requestIndex+=1

                  except Exception,ex:
                      # a problem occured, see what error it is
                      # and handle it.
                      logging.debug("ERROR: "+str(ex))
                      prodMgrBlackList[request['ProdMgrURL']]='off'
                      time.sleep(100)

               logging.debug("ProdMgrInterface state set to: cleanup")
               componentState="cleanup"

            if componentState=="cleanup":
                recover=False

                # if we have suceeded until here we can remove the workQueues and sessionQueues
                logging.debug("ResourcesAvailable Event was able to generate "+str(len(self.workAllocationQueue))+" Jobs")
                del self.workAllocationQueue 

                logging.debug("ProdMgrInterface state set to: start")
                componentState="start"
                stateParameters={'requestIndex':0}

        except Exception,ex:
           # depending on the error we need to rollback things
            rollback_all()
#            logging.debug("HANDLE THIS BY LOOKING AT THE  ERROR CODE: "+str(ex.args[0].faultCode))
            logging.debug("HANDLE THIS BY LOOKING AT THE  ERROR CODE: "+str(ex))


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
            self.priorityQueue.addRequest(requestId, prodMgr, priority)
            logging.debug("Added request. There are now "+str(len(self.priorityQueue))+" requests in the queue ")
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
                # SESSION: the message service uses the default session
                type, payload = self.ms.get()
                logging.debug("Message type: "+str(type)+", payload: "+str(payload))
                self.__call__(type, payload)
                # we want to commit after the call has been sucessfuly completed
                # as this message will tell us the state of the component when
                # it crashed.
                # SESSION: when the message service commits it uses the default session
                # SESSION: all actions in between that use the default session are commited
                # SESSION: or rolled back if necessary.
                logging.debug("Committing Event "+str(type))
                self.ms.commit()
                # SESSION: commit and close remaining sessions
                commit_all()
                close_all()
                logging.debug("Finished handling event of type "+str(type))
        except Exception,ex:
            logging.debug("ERROR: "+str(ex))     
            raise
