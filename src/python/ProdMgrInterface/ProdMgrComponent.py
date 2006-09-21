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


class ProdMgrComponent:
    """
    _ProdMgrComponent_

    Component that interacts with the ProdMgr

    """
    def __init__(self, **args):

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


        self.priorityQueue = PriorityQueue()
        

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
        try:
            logging.debug("Retrieve work for "+str(numberOfJobs)+" jobs")
            logging.debug("There are "+str(len(self.priorityQueue))+" requests in the queue ")

            if (len(self.priorityQueue)>0):
                logging.debug("Checking for allocations Step 1")
                # get request with highest priority:
                self.priorityQueue.orderRequests()
                logging.debug("Checking for allocations Step 2")
                request=self.priorityQueue[0]
                logging.debug("Checking for allocations Step 3")
                # check how many allocations we have from this request:
                idleAllocations=0
                logging.debug("Checking allocations for request "+str(request['RequestID']))
                for allocation in request['Allocations'].keys():
                    if request['Allocations'][allocation]=='idle':
                        idleAllocations+=1 
                if (idleAllocations<numberOfJobs):
                    logging.debug("Not enough idle allocations request "+request['RequestID']+\
                        ", will acquire more")
                else:
                    logging.debug("Sufficient allocations acquired, proceeding with acquiring job")
        except Exception,ex:
            raise ProdAgentException("ERROR: "+str(ex))


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
        # create message service
        self.ms = MessageService()

        # register
        self.ms.registerAs("ProdMgrInterface")

        # subscribe to messages
        self.ms.subscribeTo("ProdMgrInterface:StartDebug")
        self.ms.subscribeTo("ProdMgrInterface:EndDebug")
        self.ms.subscribeTo("ProdMgrInterface:AddRequest")
        self.ms.subscribeTo("ResourcesAvailable")
        self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("GeneralJobFailure")
        
        
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            self.__call__(type, payload)
