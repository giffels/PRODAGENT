#!/usr/bin/env python
"""
_T0ASTPlugin_

Plugin to pull in files for a dataset/run from the Tier 0 DB
and generate a DQM Harvesting workflow/job.
"""

import logging
import os
import threading
import time

from DQMInjector.Plugins.BasePlugin import BasePlugin
from DQMInjector.HarvestWorkflow import createHarvestingWorkflow

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker

from ProdAgentCore.Configuration import loadProdAgentConfiguration

class T0ASTWrapper:
    """
    _T0ASTWrapper_

    Wrapper for T0AST API and imports.
    """
    def __init__(self):
        """
        ___init___

        Create a database connection to T0AST.
        """
        try:
            from T0.GenericTier0.Tier0DB import Tier0DB
        except Exception, ex:
            msg = "Unable to import Tier 0 Python Libs"
            raise RuntimeError, msg

        paConfig = loadProdAgentConfiguration()
        t0astDBConfig = paConfig.getConfig("Tier0DB")

        self.t0astDBConn = Tier0DB(t0astDBConfig)
        self.t0astDBConn.connect()

    def listFiles(self, runNumber, primaryDataset):
        """
        _listFiles_

        Retrieve all of the RECO files from T0AST for a given primary dataset
        and run.  Files are returned in the form of LFNs.
        """
        try:
            from T0.State.Database.Reader import ListFiles
        except Exception, ex:
            msg = "Unable to import Tier 0 Python Libs"
            raise RuntimeError, msg
        
        files = ListFiles.listFilesForDQM(self.t0astDBConn, runNumber,
                                          primaryDataset)
        return files

    def listRecoConfig(self, runNumber, primaryDataset):
        """
        _listRecoConfig_
    
        Retrieve the reco configuration for a given run and dataset from
        T0AST.  The configuration is returned in the form of a dictionary
        with the following keys:
          DO_RECO - Bool, determines if reconstruction is enabled for the
                    dataset.
          GLOBAL_TAG - Global tag used for reconstruction. 
          PROC_VER - Processing version.
          CMSSW_VERSION - Framework version used for reconstruction.
          CONFIG_URL - URL to the framework config file.
        """
        try:
            from T0.State.Database.Reader import ListRunConfig
        except Exception, ex:
            msg = "Unable to import Tier 0 Python Libs"
            raise RuntimeError, msg
        
        return ListRunConfig.retrieveRecoConfigForDataset(self.t0astDBConn,
                                                          runNumber,
                                                          primaryDataset)

class T0ASTPlugin(BasePlugin):
    """
    _T0ASTPlugin_

    DQMInjector plugin used for offline DQM in the Tier0.
    """
    def __init__(self):
        """
        ___init___

        Call the base constructor and initialize some attributes.
        """
        BasePlugin.__init__(self)
        self.t0astWrapper = None

        # The Tier0 only submits jobs to CERN...
        self.site = "Default"

    def __call__(self, collectPayload):
        """
        ___call___

        Create a DQM job for then given run/dataset.
        """
        myThread = threading.currentThread()
        
        try:
            if "t0astWrapper" not in dir(myThread):
                self.t0astWrapper = T0ASTWrapper()
                myThread.t0astWrapper = self.t0astWrapper
            else:
                self.t0astWrapper = myThread.t0astWrapper
        except Exception, ex:
            msg = "Error connecting to T0AST Database and retrieving\n"
            msg += "Information for %s\n" % str(collectPayload)
            msg += str(ex)
            raise RuntimeError, msg

        (workflowSpec, workflowSpecFile) = \
                       self.createWorkflow(collectPayload["RunNumber"],
                                           collectPayload["PrimaryDataset"],
                                           collectPayload["ProcessedDataset"],
                                           collectPayload["DataTier"])

        if workflowSpec == None:
            return []

        (jobSpec, jobSpecFile) = \
                  self.createJob(workflowSpec,
                                 collectPayload["RunNumber"],
                                 collectPayload["PrimaryDataset"])

        if jobSpec == None:
            return []

        job = {}
        job["JobSpecId"] = jobSpec.parameters["JobName"]
        job["JobSpecFile"] = jobSpecFile
        job["JobType"] = "Harvesting"
        job["WorkflowSpecId"] = workflowSpec.workflowName(),
        job["WorkflowPriority"] = 10
        job["Sites"] = [self.site]
        job["Run"] = collectPayload["RunNumber"]
        job["WorkflowSpecFile"] = workflowSpecFile

        msg = "Harvesting Job Created for\n"
        msg += " => Run:       %s\n" % collectPayload["RunNumber"]
        msg += " => Primary:   %s\n" % collectPayload["PrimaryDataset"]
        msg += " => Processed: %s\n" % collectPayload["ProcessedDataset"]
        msg += " => Tier:      %s\n" % collectPayload["DataTier"]
        msg += " => Workflow:  %s\n" % job["WorkflowSpecId"]
        msg += " => Job:       %s\n" % job["JobSpecId"]
        msg += " => Site:      %s\n" % job["Sites"]
        logging.info(msg)

        return [job]
        
    def createWorkflow(self, runNumber, primaryDataset,
                       processedDataset, dataTier):
        """
        _createWorkflow_

        Create a workflow for a given run and primary dataset.  If the workflow
        has been created previously, load it and use it.
        """
        jobCache = os.path.join(self.args["ComponentDir"], "T0ASTPlugin",
                                "Run" + runNumber)
        if not os.path.exists(jobCache):
            os.makedirs(jobCache)

        workflowSpecFileName = "DQMHarvest-Run%s-%s-workflow.xml" % (runNumber, primaryDataset)
        workflowSpecPath = os.path.join(jobCache, workflowSpecFileName)

        if os.path.exists(workflowSpecPath):
            msg = "Loading existing workflow for dataset: %s\n " % primaryDataset
            msg += " => %s\n" % workflowSpecPath
            logging.info(msg)

            workflowSpec = WorkflowSpec()
            workflowSpec.load(workflowSpecPath)
            return (workflowSpec, workflowSpecPath)
            
        msg = "No workflow found for dataset: %s\n " % primaryDataset
        msg += "Looking up software version and generating workflow..."

        recoConfig = self.t0astWrapper.listRecoConfig(runNumber, primaryDataset)

        if not recoConfig["DO_RECO"]:
            logging.info("RECO disabled for dataset %s" % primaryDataset)
            return (None, None)

        globalTag = self.args.get("OverrideGlobalTag", None)
        if globalTag == None:
            globalTag = recoConfig["GLOBAL_TAG"]
            
        cmsswVersion = self.args.get("OverrideCMSSW", None)
        if cmsswVersion == None:
            cmsswVersion = recoConfig["CMSSW_VERSION"]

        datasetPath = "/%s/%s/%s" % (primaryDataset, processedDataset, dataTier)
        workflowSpec = createHarvestingWorkflow(datasetPath, self.site, 
                                                self.args["CmsPath"],
                                                self.args["ScramArch"],
                                                cmsswVersion, globalTag,
                                                self.args["ConfigFile"],
                                                self.args['DQMServer'],
                                                self.args['proxyLocation'],
                                                self.args['DQMCopyToCERN'])
        
        
        workflowSpec.save(workflowSpecPath)
        msg = "Created Harvesting Workflow:\n %s" % workflowSpecPath
        logging.info(msg)
        self.publishWorkflow(workflowSpecPath, workflowSpec.workflowName())
        return (workflowSpec, workflowSpecPath)
        
    def createJob(self, workflowSpec, runNumber, primaryDataset):
        """
        _createJob_

        Given a workflow spec, run number and primaryDataset, create a DQM
        harvesting job.
        """
        jobFiles = self.t0astWrapper.listFiles(runNumber, primaryDataset)

        if len(jobFiles) == 0:
            return (None, None)
        
        jobSpec = workflowSpec.createJobSpec()
        jobName = "DQMHarvest-Run%s-%s-%s" % (runNumber, primaryDataset,
                                              time.time())
        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Processing")
        jobSpec.parameters["RunNumber"] = runNumber
        jobSpec.addWhitelistSite(self.site)
        jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.cfgInterface.inputFiles.extend(jobFiles)

        jobCache = os.path.join(self.args["ComponentDir"], "T0ASTPlugin",
                                "Run" + runNumber)            
        jobSpecFile = os.path.join(jobCache, "%s-JobSpec.xml" % jobName)

        jobSpec.save(jobSpecFile)
        return (jobSpec, jobSpecFile)
