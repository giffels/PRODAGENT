#!/usr/bin/env python
"""
_JobCreatorComponent_

JobCreator component

"""
import logging
import os
import socket
import tarfile
import time
import urllib2
import traceback

from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.Database import Session

from MessageService.MessageService import MessageService
from MessageService.MessageServiceStatus import MessageServiceStatus
from ProdAgentCore.Configuration import prodAgentName
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.Trigger.Trigger import Trigger
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job as WEJob

from JobCreator.JCException import JCException
from JobCreator.Registry import retrieveGenerator
from JobCreator.Registry import retrieveCreator

import JobCreator.Creators
import JobCreator.Generators
import ProdAgentCore.LoggingUtils as LoggingUtils


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
        self.args['mergeMaxRetries'] = 3
        self.args['HashDirs'] = True
        self.args['LogArchStageOut'] = False
        self.args['FrontierDiagnostic'] = False
        self.args['MultipleJobsPerRun'] = False
        
        self.args.update(args)
        self.prodAgent = prodAgentName()
        self.job_state = self.args['JobState']
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        if str(self.args['LogArchStageOut']).lower() in ("true", "yes"):
            self.args['LogArchStageOut'] = True
        else:
            self.args['LogArchStageOut'] = False

        if str(self.args['FrontierDiagnostic']).lower() in ("true", "yes"):
            self.args['FrontierDiagnostic'] = True
        else:
            self.args['FrontierDiagnostic'] = False

        if str(self.args['MultipleJobsPerRun']).lower() in ("true", "yes"):
            self.args['MultipleJobsPerRun'] = True
        else:
            self.args['MultipleJobsPerRun'] = False

      


        LoggingUtils.installLogHandler(self)
        msg = "JobCreator Started:\n"
        msg += " Generator: %s\n" % self.args['GeneratorName']
        msg += " Creator: %s \n" % self.args['CreatorName']

        logging.info(msg)
        
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
                msg += "Details: %s\n" % str(ex)
                msg += "Traceback: %s\n" % traceback.format_exc()
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
                msg += "\nTraceback: %s\n" % traceback.format_exc()
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

        wfBackup = os.path.join(wfCache, "%s-%s-Workflow.xml" % (
            wfname, spec.parameters['WorkflowType'])
                                )
        spec.save(wfBackup)
        
        gen = retrieveGenerator(self.args['GeneratorName'])
        creator = retrieveCreator(self.args['CreatorName'])
        gen.creator = creator
        gen.workflowFile = workflowSpec
        gen.workflowCache = wfCache
        gen.componentConfig = self.args
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
        
        logging.debug("Site Whitelist= %s" % primaryJobSpec.siteWhitelist)
        
        if not primaryJobSpec.isBulkSpec():
            #  //
            # // Non bulk spec: handle as single spec
            #//
            jobSpecToPublish = self.handleJobSpecInstance(primaryJobSpec)
            jobname = primaryJobSpec.parameters['JobName']
            logging.debug("Publishing SubmitJob: %s" % jobname)
            logging.debug("Publishing SubmitJob:File=%s" % jobSpecToPublish)
            if jobSpecToPublish == None:
                msg = "No JobSpec Returned due to error\n"
                msg += "unable to proceed with Job Submission\n\n"
                logging.error(msg)
                return
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
            if newSpecFile == None:
                msg = "No JobSpec Returned due to error\n"
                msg += "unable to proceed with Job Submission\n\n"
                logging.error(msg)
                return
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
        #       We don't want this to go somewhere that'll eventually be cleaned up...
        #       strip off last part of dir
        bulkTar = os.path.dirname(bulkTar) 
        bulkNNNN = os.path.split(bulkTar)[1]
        bulkTar += "/BulkSpecs"
        if not os.path.exists(bulkTar):
             os.makedirs(bulkTar)
        bulkName = "/%s-%s-BulkSpecs.tar.gz" % (workflowName, int(time.time()))
        bulkTar += bulkName
        newBulkSpec.bulkSpecs.update(newSpecs)
        newBulkSpec.parameters['BulkInputSpecSandbox'] = bulkTar
        newBulkSpec.siteWhitelist.extend(primaryJobSpec.siteWhitelist)
        newBulkSpec.siteBlacklist.extend(primaryJobSpec.siteBlacklist)

        logging.debug("Setting bulk flags for trigger")
        WEJob.setBulkId(newSpecs.keys(), bulkNNNN+'::'+bulkName) 
        self.trigger.addFlag("bulkClean",bulkNNNN+'::'+bulkName,newSpecs.keys())
        self.trigger.setAction(bulkNNNN+"::"+bulkName,"bulkClean","bulkCleanAction")

        logging.debug("Bulk Spec Whitelist: %s" % newBulkSpec.siteWhitelist)
        
        
        #  //
        # // Generate job spec tarball and add to bulk job spec
        #//
        createBulkSpecTar(newBulkSpec, bulkTar)
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
        wfBackup = os.path.join(
            wfCache,
            "%s-%s-Workflow.xml" % (workflowName, jobSpec.payload.jobType))
        
        runNum = jobSpec.parameters.get("RunNumber", None)
        runPadding = None
        if runNum == None:
            runNum = abs(hash(jobname))
            runNum = "%s" % runNum

            if (jobType == 'CleanUp'):
                runPadding = "cleanups"
            elif (jobType == 'LogCollect'):
                runPadding = "logcollects"
            else:  
               runPadding = "merges"
        else:
            runNum = int(runNum)
            runPadding = str(runNum // 1000).zfill(4)

        # For T0, there will be multiple jobs occuring in a single run.  It is
        # assumed that the workflow name will contain the run number and that
        # the jobName for a particular run is unique.  The directory structure
        # for the T0 job Cache is:
        #   ComponentDir/workflowName/NNNN/jobName
        #
        # The default directory structure is:
        #   ComponentDir/workflowName/NNNN/runNumber
        if self.args["MultipleJobsPerRun"] == True:
            jobSpecDirKey = str((abs(id(jobSpec)) % 1000)).zfill(4)        
            jobCache = os.path.join(self.args['ComponentDir'],
                                    workflowName,
                                    jobSpecDirKey,
                                    jobname)
        else:
            jobCache = os.path.join(self.args['ComponentDir'],
                                    workflowName,
                                    "%s" % runPadding,
                                    str(runNum))
        
        if not os.path.exists(jobCache):
            os.makedirs(jobCache)

            
        if not WEJob.exists(jobname):
            numberOfAttempts = 0
        else:
            jobData = WEJob.get(jobname)
            numberOfAttempts = jobData.get('retries', 0)

        jobSpec.parameters['SubmissionCount'] = numberOfAttempts

        try:
            gen = retrieveGenerator(self.args['GeneratorName'])
            creator = retrieveCreator(self.args['CreatorName'])
            gen.creator = creator
            gen.componentConfig = self.args
            gen.workflowFile = wfBackup
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
            if not JobState.isRegistered(jobname):
                # FRANK (5 lines)
                logging.debug("job is NOT registered, registering...")
                if jobType == "Merge":
                    JobState.register(jobname, 'Merge',\
                                        int(self.args['mergeMaxRetries']),\
                                        1, workflowName)
                    
                else:
                    JobState.register(jobname, jobType,\
                                        int(self.args['maxRetries']),\
                                        1, workflowName)
            logging.debug("job state has been  registered")                    
            JobState.create(jobname, jobCache)
            logging.debug("job state has been created")                    
            JobState.inProgress(jobname)
            logging.debug("job state is in progress")
        except Exception, ex:
            # NOTE: we can have different errors here 
            # NOTE: transition, submission, other...
            msg = "JobState Error: %s\n" % str(ex)
            msg += "Traceback: %s\n" % traceback.format_exc()
            logging.error(msg)
            return

        try:

            cleanFlags = self.mss.isSubscribedTo("SetJobCleanupFlag") 
            logging.debug("Found following components for cleanup flags " + \
                str(cleanFlags))
            if jobType in  ("Merge", "Processing", "CleanUp"):
                cleanFlags += \
                self.mss.isSubscribedTo("MergeAccountant:SetJobCleanupFlag")
                logging.debug("Merge job: Adding MergeAccountant " +\
                    "Cleanup flag to Merge type job")
            if jobSpec.parameters.has_key("ProdMgr"):
                logging.debug(
                    "ProdMgr based job: Adding ProdMgr cleanup Flag to job")
                cleanFlags += \
                self.mss.isSubscribedTo("ProdMgrInterface:SetJobCleanupFlag") 
            for component in cleanFlags:
                logging.debug("trigger.addFlag(cleanup, %s, %s" % (
                    jobname, component)
                              )
                self.trigger.addFlag("cleanup", jobname, component)
               
            if len(cleanFlags) > 0:
                #  //
                # // Only set the action if there are components
                #//  that need it.
                logging.debug("Cleanup action installed for %s" % jobname)
                self.trigger.setAction(jobname,"cleanup","jobCleanAction")
        except Exception, ex:
            # NOTE: we can have different errors here 
            # NOTE: transition, submission, other...
            msg = "Cleanup flag Error: %s\n" % str(ex)
            msg += "Traceback: %s\n" % traceback.format_exc()
            logging.error(msg)
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
        # create message service status object
        self.mss = MessageServiceStatus()
        self.trigger=Trigger(self.ms)                                                                                
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
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("JobCreator: %s, %s" % (type, payload))
            self.__call__(type, payload)
            Session.commit_all()
            Session.close_all()

                                                                                

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

