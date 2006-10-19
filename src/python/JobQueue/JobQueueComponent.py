#!/usr/bin/env python
"""
_JobQueueComponent_



"""

import os
import time
import popen2
from MessageService.MessageService import MessageService

import logging
from logging.handlers import RotatingFileHandler

import JobQueue.JobQueueAPI  as JobQueueAPI
from ProdAgentCore.ResourceConstraint import ResourceConstraint


class JobQueueComponent:
    """
    _JobQueueComponent_


    """
    def __init__(self, **args):
        self.args = {}
        self.args.setdefault("Logfile", None)
        self.args.setdefault("ProcessingPriority", 10)
        self.args.setdefault("MergePriority", 15)
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
        
        logging.info("JobQueueComponent Started...")

        
        
                              
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)

        if event == "ResourcesAvailable":
            self.resourcesAvailable(payload)
            return

        if event == "QueueJob":
            self.queueJob(payload)
            return
        
        if event == "JobQueue:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "JobQueue:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        
        return

    def queueJob(self, jobSpecFile):
        """
        _queueJob_

        Add the job spec provided to the JobQueue

        """
        priorities = { "Merge" : int(self.args['MergePriority']),
                       "Processing" : int(self.args['ProcessingPriority']),
                       }

        try:
            JobQueueAPI.queueJob(jobSpecFile, priorities)
        except Exception, ex:
            msg = "Queueing JobSpec Failed:\n%s\n" % jobSpecFile
            msg += str(ex)
            logging.error(msg)

        return
        

    def resourcesAvailable(self, resourceDescription):
        """
        _resourcesAvailable_

        We have resources, look to see whatever the description
        of those resources is, then act on it

        """
        constraint = ResourceConstraint()
        constraint.parse(resourceDescription)

        logging.debug(
            "Constraint Created for ResourcesAvailable: %s" % constraint)
        


    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
        # create message service
        self.ms = MessageService()
        # register
        self.ms.registerAs("JobQueue")
                                                                              
        # subscribe to messages
        self.ms.subscribeTo("JobQueue:StartDebug")
        self.ms.subscribeTo("JobQueue:EndDebug")
        self.ms.subscribeTo("ResourcesAvailable")
        self.ms.subscribeTo("QueueJob")
        
        
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("JobQueueComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)
                                                                               

            
