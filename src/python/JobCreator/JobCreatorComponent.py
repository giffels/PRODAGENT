#!/usr/bin/env python
"""
_JobCreatorComponent_

JobCreator component

"""
import socket
import urllib2
import logging

import os
import tarfile
import time

import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdAgentCore.Configuration import prodAgentName

from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from JobCreator.Registry import retrieveGenerator
from JobCreator.Registry import retrieveCreator

from JobCreator.JCException import JCException
from MessageService.MessageService import MessageService
from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI
from Trigger.TriggerAPI.TriggerAPI import TriggerAPI

import JobCreator.Creators
import JobCreator.Generators
from ProdAgentDB import Session



class JobCreatorComponent:
    """
    _JobCreatorComponent_

    ProdAgent Component that responds to CreateJob Events to generate
    jobs and submit them

    """
    def __init__(self, **args):
        self.args = {}
        self.args['CreatorName'] = "testCreator"
        self.args['GeneratorName'] = "Default"
        self.args['Logfile'] = None
        self.args['JobState'] = True
        self.args['maxRetries'] = 3
        #FRANK (one line)
        self.args['mergeMaxRetries'] = 3
        self.args['HashDirs'] = True
        self.args.update(args)
        self.prodAgent = prodAgentName()
        self.job_state = self.args['JobState']
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        LoggingUtils.installLogHandler(self)
        msg = "JobCreator Started:\n"
        msg += " Generator: %s\n" % self.args['GeneratorName']
        msg += " Creator: %s \n" % self.args['CreatorName']

        logging.info(msg)
        
        #  //
        # // Components needing cleanup flags set for each job
        #//  TODO: get this from configuration somehow...
        self.cleanupFlags = ['StatTracker', 'DBSInterface']
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define response to an Event and payload

        """
        msg = "Recieved Event: %s" % event
        msg += "Payload: %s" % payload
        msg += "Current Creator: %s" % self.args['CreatorName']
        logging.debug(msg)

        
        if event == "CreateJob":
            logging.info("Creating Job %s" % payload)
            try:
                self.createJob(payload)
                return
            except StandardError, ex:
                msg = "Failed to Create Job: %s\n" % payload
                msg += "Details: %s" % str(ex)
                logging.error(msg)
                return

        if event == "NewWorkflow":
            logging.info("JobCreator:NewWorkflow: %s" % payload)
            try:
                self.newWorkflow(payload)
                return
            except Exception, ex:
                msg = "Failed to handle NewWorkflow: %s\n" % payload
                msg += str(ex)
                logging.error(msg)
                return
            
        elif event == "JobCreator:SetCreator":
            #  //
            # // Payload should be name of registered creator
            #//
            self.setCreator(payload)
            logging.debug("Set Creator: %s" % payload)
            return

        elif event == "JobCreator:SetGenerator":
            #  //
            # // Payload should be name of generator
            #//
            self.setGenerator(payload)
            logging.debug("Set Generator: %s" % payload)
            return


        elif event == "JobCreator:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "JobCreator:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        

    def newWorkflow(self, workflowSpec):
        """
        _newWorkflow_

        Read the new workflow file and generate a cache area for it

        """
        spec = WorkflowSpec()
        try:
            spec.load(workflowSpec)
        except Exception, ex:
            msg = "Unable to read WorkflowSpec file:\n"
            msg += "%s\n" % workflowSpec
            msg += str(ex)
            logging.error(msg)
            return

        wfname = spec.workflowName()
        wfCache = os.path.join(self.args['ComponentDir'],
                               wfname)
        if not os.path.exists(wfCache):
            os.makedirs(wfCache)

        
        gen = retrieveGenerator(self.args['GeneratorName'])
        creator = retrieveCreator(self.args['CreatorName'])
        gen.creator = creator
        gen.workflowCache = wfCache
        gen.actOnWorkflowSpec(spec, wfCache)

        return
    
        
        
            
    def setCreator(self, creatorName):
        """
        _setCreator_

        Allow dynamic changing of Creator plugin. Sets the CreatorName
        to the value provided.

        """
        self.args['CreatorName'] = creatorName
        return

    def setGenerator(self, generatorName):
        """
        _setCreator_

        Allow dynamic changing of Generator plugin. Sets the GeneratorName
        to the value provided.

        """
        self.args['GeneratorName'] = generatorName
        return



    def createJob(self, jobSpecFile):
        """
        _createJob_

        Create a processing job based on the jobSpec instance provided

        """
        logging.debug("Reading JobSpec: %s" % jobSpecFile)
        primaryJobSpec = self.readJobSpec(jobSpecFile)
        if primaryJobSpec == None:
            logging.error("Unable to Create Job for: %s" % jobSpecFile)
            return

        
        
        if not primaryJobSpec.isBulkSpec():
            #  //
            # // Non bulk spec: handle as single spec
            #//
            jobSpecToPublish = self.handleJobSpecInstance(primaryJobSpec)
            jobname = primaryJobSpec.parameters['JobName']
            logging.debug("Publishing SubmitJob: %s" % jobname)
            self.ms.publish("AcceptedJob", jobname)
            self.ms.publish("SubmitJob", jobSpecToPublish)
            self.ms.commit()
            return
        
        #  //
        # // Still here => Bulk Job spec, so process each spec,
        #//  regenerate the bulk spec file and publish that.
        logging.info("Bulk Spec provided containing %s jobs" % (
            len(primaryJobSpec.bulkSpecs.keys()),
            )
                     )

        firstSpec = None
        newSpecs = {}
        for specID, specFile in primaryJobSpec.bulkSpecs.items():
            specInstance = self.readJobSpec(specFile)
            logging.debug("Bulk Spec: %s From %s" % (specID, specFile))
            if specInstance == None:
                msg = "Unable to load bulk Job Spec:\n%s\n" % specFile
                logging.warning(msg)
                continue
            
            newSpecFile = self.handleJobSpecInstance(specInstance)
            newSpecs[specID] = newSpecFile
            if firstSpec == None:
                firstSpec = newSpecFile


        #  //
        # // Now update and save the bulk spec with the new spec 
        #//  file locations and then publish it for submission
        logging.debug("Converting to Bulk Spec: %s" % firstSpec)
        newBulkSpec = self.readJobSpec(firstSpec)
        newBulkSpecFile = "%s.BULK" % firstSpec
        workflowName = newBulkSpec.payload.workflow
        
        bulkTar = os.path.dirname(firstSpec)
        bulkTar += "/%s-%s-BulkSpecs.tar.gz" % (workflowName, int(time.time()))
        newBulkSpec.bulkSpecs.update(newSpecs)
        
        
        #  //
        # // Generate job spec tarball and add to bulk job spec
        #//
        createBulkSpecTar(newBulkSpec, bulkTar)
        newBulkSpec.parameters['BulkInputSpecSandbox'] = bulkTar
        newBulkSpec.save(newBulkSpecFile)
        
        #  //
        # // Publish AcceptedJob events to allow cleanup
        #//
        for specId in newSpecs.keys():
            logging.debug("Publishing AcceptedJob: %s" % specId)
            self.ms.publish("AcceptedJob", specId)
            self.ms.commit()
        
        logging.debug("Publishing (BulkSpec) SubmitJob: %s" % newBulkSpecFile)
        self.ms.publish("SubmitJob", newBulkSpecFile)
        self.ms.commit()
        return
        



    def handleJobSpecInstance(self, jobSpec):
        """
        _handleJobSpecInstance_

        Operate on a JobSpec Instance to create a Job Cache

        """
        jobname = jobSpec.parameters['JobName']
        jobType = jobSpec.parameters['JobType']
        jobSpec.parameters['ProdAgent'] = self.prodAgent
        workflowName = jobSpec.payload.workflow
        wfCache = os.path.join(self.args['ComponentDir'],
                               workflowName)
        
        if self.args['HashDirs']:
            runNum = jobSpec.parameters.get("RunNumber", None)
            if runNum == None:
                runNum = abs(hash(jobname))
                runNum = "m%s" % runNum
            jobCache = os.path.join(self.args['ComponentDir'],
                                    workflowName,
                                    str(runNum))
        else:
            jobCache = os.path.join(self.args['ComponentDir'],
                                    workflowName,
                                    jobname)

            
        
        if not os.path.exists(jobCache):
            os.makedirs(jobCache)

            
        try:
            gen = retrieveGenerator(self.args['GeneratorName'])
            creator = retrieveCreator(self.args['CreatorName'])
            gen.creator = creator
            gen.workflowCache = wfCache
            gen.jobCache = jobCache
            newJobSpec = gen.actOnJobSpec(jobSpec, jobCache)
        except Exception, ex:
            logging.error("Failed to create Job: %s\n%s" % (jobname, ex))
            self.ms.publish("CreateFailed", jobname)
            self.ms.commit()
            return


        try:
            #  // 
            # // Register job creation for jobname, provide Cache Area
            #//  and set job state to InProgress

            if not JobStateInfoAPI.isRegistered(jobname):
                # FRANK (5 lines)
                if jobType == "Merge":
                    JobStateChangeAPI.register(jobname, 'Merge',\
                                        int(self.args['mergeMaxRetries']),\
                                        1)
                else:
                    JobStateChangeAPI.register(jobname, 'Processing',\
                                        int(self.args['maxRetries']),\
                                        1)
            JobStateChangeAPI.create(jobname, jobCache)
            JobStateChangeAPI.inProgress(jobname)
            
            logging.debug(
                " Adding cleanup triggers for %s" % self.cleanupFlags
                )
          
        except Exception, ex:
            # NOTE: we can have different errors here 
            # NOTE: transition, submission, other...
            logging.error("JobState Error:%s" % str(ex))
            return

        try:
            cleanFlags = []
            cleanFlags.extend(self.cleanupFlags)
            if jobType == "Merge":
                logging.debug(
                    "Adding MergeAccountant Cleanup Flag to Merge type job")
                cleanFlags.append("MergeAccountant")

            for component in cleanFlags:
                logging.debug("trigger.addFlag(cleanup, %s, %s" % (
                    jobname, component)
                              )
                self.trigger.addFlag("cleanup", jobname, component)

            #NOTE: this is a check in case we use the ProdMgrInterface
            logging.debug(
                "Checking if job %s is associated to prodmgr" % jobname)
            if jobSpec.parameters.has_key("ProdMgr"):
                logging.debug(
                    "Job constructed using ProdMgr, adding extra trigger")
                self.trigger.addFlag("cleanup", jobname, "ProdMgrInterface")
            #NOTE: we need to make sure we commit and close this connection
            #NOTE: eventually this needs to be the same commit/close
            #NOTE: as the message service.
            #END NOTE
               
            if len(cleanFlags) > 0:
                #  //
                # // Only set the action if there are components
                #//  that need it.
                self.trigger.setAction(jobname,"cleanup","jobCleanAction")
        except Exception, ex:
            # NOTE: we can have different errors here 
            # NOTE: transition, submission, other...
            logging.error("Cleanup flag Error:%s" % str(ex))
            return
        
        
        return newJobSpec
        

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
        self.ms.subscribeTo("NewWorkflow")
        self.ms.subscribeTo("JobCreator:SetCreator")
        self.ms.subscribeTo("JobCreator:SetGenerator")
        self.ms.subscribeTo("JobCreator:StartDebug")
        self.ms.subscribeTo("JobCreator:EndDebug")
 
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("JobCreator: %s, %s" % (type, payload))
            self.__call__(type, payload)
                                                                                

def createBulkSpecTar(bulkSpec, tarfileName):
    """
    _createBulkSpecTar_

    Given a Bulk spec that contains N specs, create a
    tarball with the name provided in the directory provided.

    """
    tarball = tarfile.open(tarfileName, "w:gz")

    for id, filename in bulkSpec.bulkSpecs.items():
        tarball.add(filename, "BulkSpecs/%s" % os.path.basename(filename), False)

    tarball.close()
    return 

