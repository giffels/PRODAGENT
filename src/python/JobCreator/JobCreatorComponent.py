#!/usr/bin/env python
"""
_JobCreatorComponent_

JobCreator component

"""
import socket
import urllib2
import logging

import os


import ProdAgentCore.LoggingUtils as LoggingUtils

from MCPayloads.JobSpec import JobSpec
from JobCreator.JobGenerator import JobGenerator
from JobCreator.JCException import JCException
from MessageService.MessageService import MessageService
from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI
from Trigger.TriggerAPI.TriggerAPI import TriggerAPI

import JobCreator.Creators


class JobCreatorComponent:
    """
    _JobCreatorComponent_

    ProdAgent Component that responds to CreateJob Events to generate
    jobs and submit them

    """
    def __init__(self, **args):
        self.args = {}
        self.args['CreatorName'] = "testCreator"
        self.args['Logfile'] = None
        self.args['JobState'] = True
        self.args['maxRetries'] = 3
        self.args.update(args)
        self.job_state = self.args['JobState']
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        LoggingUtils.installLogHandler(self)
        logging.info("JobCreator Component Started...")

        #  //
        # // Components needing cleanup flags set for each job
        #//  TODO: get this from configuration somehow...
        self.cleanupFlags = ['StatTracker', 'DBSInterface']

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to an Event and payload

        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)
        logging.debug("Current Creator: %s" % self.args['CreatorName'])

        
        if event == "CreateJob":
            logging.info("Creating Job %s" % payload)
            try:
                self.createJob(payload)
                return
            except StandardError, ex:
                logging.error("Failed to Create Job: %s" % payload)
                logging.error("Details: %s" % str(ex))
                return
            
        elif event == "JobCreator:SetCreator":
            #  //
            # // Payload should be name of registered creator
            #//
            self.setCreator(payload)
            logging.debug("Set Creator: %s" % payload)
            return

        elif event == "JobCreator:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "JobCreator:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        
        
            
    def setCreator(self, creatorName):
        """
        _setCreator_

        Allow dynamic changing of Creator plugin. Sets the CreatorName
        to the value provided.

        """
        self.args['CreatorName'] = creatorName
        return


    def createJob(self, jobSpecFile):
        """
        _createJob_

        Create a processing job based on the jobSpec instance provided

        """
        logging.debug("Reading JobSpec: %s" % jobSpecFile)
        jobSpec = self.readJobSpec(jobSpecFile)
        if jobSpec == None:
            logging.error("Unable to Create Job for: %s" % jobSpecFile)
            return
        jobname = jobSpec.parameters['JobName']
        jobType = jobSpec.parameters['JobType']
        #  //
        # // Initialise the JobBuilder
        #//
        logging.debug("Instantiating Generator for JobSpec: %s" % jobname)
        jobGen = JobGenerator(jobSpec, self.args)
        #  //
        # // Create the Job
        #//
        try:
            logging.debug("Calling Generator...")
            cacheArea = jobGen()
        except StandardError, ex:
            logging.error("Failed to create Job: %s\n%s" % (jobname, ex))
            self.ms.publish("CreateFailed", jobname)
            self.ms.commit()
            return
        
        if self.job_state:
            try:
                #  // 
                # // Register job creation for jobname, provide Cache Area
                #//  and set job state to InProgress

                # NOTE: racers is fixed but should
                # NOTE: configurable

                # NOTE: does this component only handle processing jobs?
                # NOTE: if not we need to differentiate between processing
                # NOTE: and merging jobs
                
                # we only register once. The second time will give
                # an error which we will pass. Historically registration
                # was part of the request injector, and would not clash
                # with re-job creation.
                if not JobStateInfoAPI.isRegistered(jobname):
                    JobStateChangeAPI.register(jobname, 'Processing',
                                               int(self.args['maxRetries']),
                                               1)
                JobStateChangeAPI.create(jobname, cacheArea)
                JobStateChangeAPI.inProgress(jobname)

                logging.debug(" Adding cleanup triggers for %s" % self.cleanupFlags)

                cleanFlags = []
                cleanFlags.extend(self.cleanupFlags)
                if jobType == "Merge":
                    logging.debug("Adding MergeSensor Cleanup Flag to Merge type job")
                    cleanFlags.append("MergeSensor")

                for component in cleanFlags:
                    logging.debug("trigger.addFlag(cleanup, %s, %s" % (
                        jobname, component)
                                  )
                    self.trigger.addFlag("cleanup", jobname, component)
                    
                if len(cleanFlags) > 0:
                    #  //
                    # // Only set the action if there are components
                    #//  that need it.
                    self.trigger.setAction(jobname,"cleanup","jobCleanAction")
                    
                
            except Exception, ex:
                # NOTE: we can have different errors here 
                # NOTE: transition, submission, other...
                logging.error("JobState Error:%s" % str(ex))
                return
        
        #  //
        # // Publish SubmitJob event
        #//
        logging.debug("Publishing SubmitJob: %s" % jobname)
       
        self.ms.publish("SubmitJob", jobname)
        self.ms.commit()
        return
        

    def readJobSpec(self, url):
        """
        _readJobSpec_

        """
        jobSpec = JobSpec()
        try:
            jobSpec.load(url)
        except StandardError, ex:
            logging.error("Error loading JobSpec File: %s" % url)
            logging.error(str(ex))
            return None
        return jobSpec
        
        
        
        

        

    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """

 
        # create message service
        self.ms = MessageService()
        self.trigger=TriggerAPI(self.ms)                                                                                
        # register
        self.ms.registerAs("JobCreator")

        # subscribe to messages
        self.ms.subscribeTo("CreateJob")
        self.ms.subscribeTo("JobCreator:SetCreator")
        self.ms.subscribeTo("JobCreator:StartDebug")
        self.ms.subscribeTo("JobCreator:EndDebug")
 
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("JobCreator: %s, %s" % (type, payload))
            self.__call__(type, payload)
                                                                                

