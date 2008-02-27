#!/usr/bin/env python
"""
_JobQueueComponent_



"""

import os
import time
import popen2
from MessageService.MessageService import MessageService
from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig

import logging
from logging.handlers import RotatingFileHandler

import JobQueue.JobQueueAPI  as JobQueueAPI
import JobQueue.Prioritisers
from JobQueue.Registry import retrievePrioritiser
from JobQueue.BulkSorter import BulkSorter

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
        self.args.setdefault('BulkMode', True)
        self.args.setdefault('ExpireConstraints', 7200)
        self.args.update(args)

        if self.args['Logfile'] == None:
           self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        if str(self.args['BulkMode']).lower() == "false":
            self.args['BulkMode'] = False 
        else:
            self.args['BulkMode'] = True

        self.args['ExpireConstraints'] = int(self.args['ExpireConstraints'])
        LoggingUtils.installLogHandler(self)

        msg = "JobQueueComponent Started:\n"
        msg += " ==> ProcessingPriority = "
        msg += "%s\n" % self.args['ProcessingPriority']
        msg += " ==> MergePriority = "
        msg += "%s\n" % self.args['MergePriority']
        msg += " ==> Prioritiser = "
        msg += "%s\n" % self.args['Prioritiser']
        msg += "Constraints will be considered expired after:\n"
        msg += " ==>%s\n" % self.args['ExpireConstraints'] 
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
            logging.info("----- Queueing Job: %s" % jobSpecFile)
        except Exception, ex:
            msg = "Queueing JobSpec Failed:\n%s\n" % jobSpecFile
            msg += str(ex)
            logging.error(msg)

        logging.debug("queueingJob: %s" % jobSpecFile)

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
        
        timestamp = constraint.get('ts', None)
        if timestamp != None:
            timenow = int(time.time())
            timediff = timenow - timestamp
            if timediff > self.args['ExpireConstraints']:
                msg = "ResourcesAvailable Constraint is Expired:\n"
                msg += "Constraint Timestamp = %s\n" % timestamp
                msg += "Is older than %s seconds\n" % (
                    self.args['ExpireConstraints'],)
                msg += "Event will be ignored..."
                logging.warning(msg)
                return

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

        if len(jobspecs) > 0:
            siteOverride = constraint.get("site", None)
            self.sortAndPublish(siteOverride, *jobspecs)
        
        return

    def sortAndPublish(self, siteOverride = None, *jobspecs):
        """
        _sortAndPublish_

        Sort and publish CreateJob events, making bulk specs if required
        
        """
        logging.info("siteOverride=%s" % siteOverride)
        
        if self.args['BulkMode'] == False:
            #  //
            # // publish specs individually
            #//
            for job in jobspecs:
                if siteOverride != None:
                    self.overrideSite(job['JobSpecFile'], siteOverride)
                logging.info("publishing CreateJob %s" % job['JobSpecFile'])
                self.ms.publish("CreateJob", job['JobSpecFile'])
                self.ms.commit()
            JobQueueAPI.releaseJobs(*jobspecs)
            return
        #  //
        # // sort into bulk specs based on same workflow, site and job type
        #//  values
        sorter = BulkSorter()
        sorter(*jobspecs)
        
        for indSpec in sorter.individualSpecs:
            if siteOverride != None:
                self.overrideSite(indSpec['JobSpecFile'], siteOverride)
            logging.info("publishing  CreateJob %s" % indSpec['JobSpecFile'])
            self.ms.publish("CreateJob", indSpec['JobSpecFile'])
            self.ms.commit()
            JobQueueAPI.releaseJobs(indSpec)
            

        for bulkSpecList in sorter.bulkSpecs.values():
            bulkSpecs = {}
            [ bulkSpecs.__setitem__(x['JobSpecFile'], x['JobSpecId'])
              for x in bulkSpecList ]
            firstSpec = bulkSpecs.keys()[0]
            logging.debug("firstSpec=%s" % firstSpec)
            bulkSpecName = "%s.BULK" % firstSpec
            bulkSpecName = bulkSpecName.replace("file:///", "/")
            logging.info("Bulk Spec: %s" % bulkSpecName)
            bulkSpec = JobSpec()
            firstSpecName = firstSpec.replace("file:///", "/")
            if not os.path.exists(firstSpecName):
                msg = "Primary Spec for Bulk Spec creation not found:\n"
                msg += "%s\n" % firstSpecName
                msg += "Cannot construct Bulk Spec"
                logging.error(msg)
                continue
            bulkSpec.load(firstSpec)
            for specFile, specId in bulkSpecs.items():
                bulkSpec.bulkSpecs.addJobSpec(specId, specFile)
            if siteOverride != None:
                bulkSpec.siteWhitelist = []
                bulkSpec.siteWhitelist.append(siteOverride)
                
            bulkSpec.save(bulkSpecName)
            logging.info("Publishing Bulk Spec")
            self.ms.publish("CreateJob", bulkSpecName)
            self.ms.commit()
            
            JobQueueAPI.releaseJobs(*bulkSpecList)
            
        
        return

    def overrideSite(self, jobSpecFile, site):
        """
        _overrideSite_

        Override the site in a jobspec file

        """
        jobSpec = JobSpec()
        try:
            jobSpec.load(jobSpecFile)
            jobSpec.siteWhitelist = []
            jobSpec.siteWhitelist.append(site)
            jobSpec.save(jobSpecFile)
            logging.info(
                "Site Overridden to %s by JobQueue for spec:\n %s" % (
                site, jobSpecFile)
                )
        except Exception, ex:
            msg = "Error: Unable to load JobSpec file:\n"
            msg += "%s\n" % jobSpecFile
            msg += "%s\n" % str(ex)
            msg += "Cannot override site..."
            logging.error(msg)

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
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("JobQueueComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)
            Session.commit_all()
            Session.close_all()
            
            
            
                                                                               

            
