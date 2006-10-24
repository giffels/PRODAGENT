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
import JobQueue.Prioritisers
from JobQueue.Registry import retrievePrioritiser

from ProdAgentCore.ResourceConstraint import ResourceConstraint
import ProdAgentCore.LoggingUtils as LoggingUtils

class JobQueueComponent:
    """
    _JobQueueComponent_


    """
    def __init__(self, **args):
        self.args = {}
        self.args.setdefault("Logfile", None)
        self.args.setdefault("ProcessingPriority", 10)
        self.args.setdefault("MergePriority", 15)
        self.args.setdefault('Prioritiser', "Default")
        self.args.update(args)

        if self.args['Logfile'] == None:
           self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        LoggingUtils.installLogHandler(self)

        msg = "JobQueueComponent Started:\n"
        msg += " ==> ProcessingPriority = "
        msg += "%s\n" % self.args['ProcessingPriority']
        msg += " ==> MergePriority = "
        msg += "%s\n" % self.args['MergePriority']
        msg += " ==> Prioritiser = "
        msg += "%s\n" % self.args['Prioritiser']
        logging.info(msg)

        
        
                              
        
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
        if event == "JobQueue:SetPrioritiser":
            self.args['Prioritiser'] = payload
            return

        if event == "JobQueue:SetMergePriority":
            try:
                self.args['MergePriority'] = int(payload)
            except ValueError, ex:
                msg = "Unable to set merge prioroty, "
                msg += "payload is not an integer"
                logging.error(msg)
            return
        if event == "JobQueue:SetProcessingPriority":
            try:
                self.args['ProcessingPriority'] = int(payload)
            except ValueError, ex:
                msg = "Unable to set processing prioroty, "
                msg += "payload is not an integer"
                logging.error(msg)
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

        logging.debug(
            "Loading Prioritiser Plugin: %s" % self.args['Prioritiser'])

        try:
            plugin = retrievePrioritiser(self.args['Prioritiser'])
            logging.debug("Plugin loaded successfully")
        except Exception, ex:
            msg = "Error loading Prioritiser Plugin:"
            msg += " %s\n" % self.args['Prioritiser']
            msg += str(ex)
            logging.error(msg)
            return
        logging.debug("Invoking Plugin on constraint")
        try:
            jobspecs = plugin(constraint)
            logging.debug("Plugin returned %s job specs" % len(jobspecs))
        except Exception, ex:
            msg = "Error Calling Prioritiser Plugin:"
            msg += " %s\n" % self.args['Prioritiser']
            msg += " On constraint: %s\n" % constraint
            msg += str(ex)
            logging.error(msg)
            return

        for job in jobspecs:
            logging.info("publishing CreateJob %s" % job)
            self.ms.publish("CreateJob", job)
            self.ms.commit()
            
        return
            
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
        self.ms.subscribeTo("JobQueue:SetPrioritiser")
        self.ms.subscribeTo("JobQueue:SetMergePriority")
        self.ms.subscribeTo("JobQueue:SetProcessingPriority")
        self.ms.subscribeTo("ResourcesAvailable")
        self.ms.subscribeTo("QueueJob")
        
        
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("JobQueueComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)
                                                                               

            
