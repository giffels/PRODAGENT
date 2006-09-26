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

from ProdMgrInterface.PriorityQueue import PriorityQueue
from ProdMgrInterface.PriorityQueue import AllocationQueue
from ProdMgrInterface.PriorityQueue import JobQueue
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
    
            # priorities hold requests while the 
            # job queue holds job information 
            self.priorityQueue          = PriorityQueue()
            self.allocationQueue        = AllocationQueue()
            self.sessionAllocationQueue = AllocationQueue()
            self.sessionJobQueue               = JobQueue()
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
        

        #NOTE: the states should be made persistent in the database.
        logging.debug("ProdMgrInterface state set to: acquireAllocations")
        componentState="acquireAllocations" 

        try:
            logging.debug("Retrieve last uncommitted call made by this component")
            retrievedResult=ProdMgrAPI.retrieve()
            serviceCall=retrieveResult[1]
            # we crashed while acquiring allocations
            if serviceCall=="prodMgrProdAgent.acquireAllocation":
                 logging.debug("Recovering results for acquireAllocation call")
                 #NOTE: insert appropiate actions here
                 logging.debug("ProdMgrInterface state set to: acquireAllocations")
                 componentState="acquireAllocations"
            if serviceCall=="prodMgrProdAgent.acquireJob":
                 logging.debug("Recovering results for acquireJob call")
                 #NOTE: insert appropiate actions here
                 logging.debug("ProdMgrInterface state set to: acquireJobs")
                 componentState="acquireAllocations"
        except:
            logging.debug("No uncommited call available")


        try:
            logging.debug("Retrieve work for "+str(numberOfJobs)+" jobs")
            logging.debug("There are "+str(len(self.priorityQueue))+" requests in the queue ")
           
            if componentState=="acquireAllocations": 
                # a mapping we use later on for job allocations
                # so we do not have to query the database twice.
                request2URL={} 
                requestIndex=0
    
                # order requests as we want to take higher priorities first.
                self.priorityQueue.orderRequests()
    
                while len(self.sessionAllocationQueue)<int(numberOfJobs):
                    # check if there are requests left:
                    if (len(self.priorityQueue)<(requestIndex+1)):
                        #NOTE: anything else we need to do here?
                        break
                    # get request with highest priority:
                    request=self.priorityQueue[requestIndex]
    
                    # make a request2 url mapping 
                    request2URL[request['RequestID']]=request['ProdMgrURL']
    
                    # check how many allocations we have from this request:
                    logging.debug("Checking allocations for request "+str(request['RequestID']))
                    idleRequestAllocations=self.allocationQueue.getIdle(request['RequestID'])
                    # do we have enought total allocations?
                    if ( (len(idleRequestAllocations)+len(self.sessionAllocationQueue) )<numberOfJobs):
                        logging.debug("Not enough idle allocations request "+request['RequestID']+\
                            ", will acquire more")
                        logging.debug("Contacting: "+request['ProdMgrURL']+" with payload "+\
                            request['RequestID']+','+str(int(numberOfJobs)-len(idleRequestAllocations)-len(self.sessionAllocationQueue)))
                        # only request the extra allocations if we have spare idle ones.
                        # the return format is an array of allocations ids
                        # we might get less allocations back then we asked for, if there are not 
                        # enought available.
    
                        allocations=ProdMgrAPI.acquireAllocation(request['ProdMgrURL'],\
                            request['RequestID'],int(numberOfJobs)-len(idleRequestAllocations)-len(self.sessionAllocationQueue))
                        # check if we got allocations back
                        # NOTE: we need some way to detect that this request is finished
                        if type(allocations)==bool:
                            if not allocations:
                                self.priorityQueue.delRequest(request['RequestID']) 
                                # request has finished
                                # NOTE: remove request from our queue
                        else: 
                            logging.debug("Acquired allocations: "+str(allocations)) 
                            # we acquired allocations, update our own accounting:
                            self.allocationQueue.add(allocations)
                            idleRequestAllocations=self.allocationQueue.getIdle(request['RequestID'])
                            self.sessionAllocationQueue.add(idleRequestAllocations) 
                        # we can commit as we made everything persistent at the client side.
                        ProdMgrAPI.commit()
                    else:
                        self.sessionAllocationQueue.add(idleRequestAllocations) 
                        logging.debug("Sufficient allocations acquired, proceeding with acquiring jobs")
                    logging.debug("Currently "+str(len(self.sessionAllocationQueue))+" available")
                    # if necessary check the other queued requests for allocations.
                    requestIndex+=1

                if(len(self.sessionAllocationQueue)==int(numberOfJobs)):
                    logging.debug("Able to acquire requested allocations. Start job allocation with allocations:")
                    logging.debug(str(self.sessionAllocationQueue))
                else:
                    logging.debug("Acquired less allocations than requested. Start job allocation with allocations")
                    logging.debug(str(self.sessionAllocationQueue))
                    # NOTE: what else do we want to do here? 

            logging.debug("ProdMgrInterface state set to: acquireJobs")
            componentState="acquireJobs" 

            if(len(self.sessionAllocationQueue)>0) and componentState=="acquireJobs":
                # we now know how many allocations we want now count how many we have per request.
                # NOTE: should have been made persistent in the database:
                counter={}
                for allocation in self.sessionAllocationQueue:
                    part=allocation['AllocationID'].split('/')
                    # part 0 is the request id
                    if not counter.has_key(part[0]):
                        counter[part[0]]=0
                    counter[part[0]]+=1
                for request_id in counter.keys():
                    parameters={'numberOfJobs':counter[request_id],
                                'prefix':'job'}   
                    # only acquire jobs if we do not have them already:
                    # it might be that during a crash we already acquired some jobs:
                    # query the persistent job queue for that
                    # e.g. : if not self.sessionJobQueue.hasJob(request_id):
                    if not self.sessionJobQueue.hasJobs(request_id):
                        jobs=ProdMgrAPI.acquireJob(request2URL[request_id],request_id,parameters)
                        self.sessionJobQueue.add(jobs)
                        ProdMgrAPI.commit()
                    #NOTE: now if jobs are conistent and the prodagent crashes, the first part of
                    #NOTE: of code of this method becomes important as we first look if there are any
                    #NOTE; jobs waiting.
                    logging.debug("Acquired the following jobs: "+str(self.sessionJobQueue)) 
                    # we have the jobs, lets download there job specs and if finished emit
                    # a new job event. 

            logging.debug("ProdMgrInterface state set to: downloadJobSpecs")
            componentState="downloadJobSpecs"

            # we are now in a state where we no longer need the session allocation queue,
            # therefor delete it.
            self.sessionAllocationQueue=AllocationQueue()

            if componentState=="downloadJobSpecs":
                   logging.debug("Downloading files to : "+self.args['JobSpecDir'])
                   for job in self.sessionJobQueue:
                       targetDir=self.args['JobSpecDir']+'/'+job['JobSpecID'].replace('/','_')
                       try:
                           os.makedirs(targetDir)
                       except:
                           pass
                       targetFile=job['JobSpecURL'].split('/')[-1]
                       logging.debug("Downloading: "+str(job['JobSpecURL']))
                       ProdMgrAPI.retrieveFile(job['JobSpecURL'],targetDir+'/'+targetFile)

            logging.debug("ProdMgrInterface state set to: emit JobSubmission events")
            componentState="jobSubmission"

           
                     
            logging.debug("ProdMgrInterface state set to: start")
            componentState="start"
            # we are now in a state where we no longer need the session job queue
            # therefore delete it:
            self.sessionJobQueue=JobQueue()

        except Exception,ex:
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
                type, payload = self.ms.get()
                logging.debug("Message type: "+str(type)+", payload: "+str(payload))
                self.__call__(type, payload)
                # we want to commit after the call has been sucessfuly completed
                # as this message will tell us the state of the component when
                # it crashed.
                self.ms.commit()
        except Exception,ex:
            logging.debug("ERROR: "+str(ex))     
            raise
