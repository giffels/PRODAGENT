#!/usr/bin/env python
"""
_RelValPlugin_

Plugin to pull in files for a dataset/run from the Tier 0 DB
and generate a DQM Harvesting workflow/job

"""
import logging
import os
import time

from DQMInjector.Plugins.BasePlugin import BasePlugin
from DQMInjector.HarvestWorkflow import createHarvestingWorkflow

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader

from ProdAgentCore.Configuration import loadProdAgentConfiguration


def findVersionForDataset(dbsUrl, primary, processed, tier, run):
    """
    _findVersionForDataset_

    Find CMSSW version used for producing the dataset.

    """
    reader  = DBSReader(dbsUrl)
    datasetName = "/%s/%s/%s" % (primary, processed, tier)

    try:
        fileList = reader.dbs.listFiles(
            path = datasetName,
            runNumber = run,
            retriveList = ['retrive_algo',])
    except Exception, ex:
        msg = "Failed to get details from DBS for dataset:\n"
        msg += "%s for run %s\n" % (datasetName, run)
        msg += "Cannot extract CMSSW Version from DBS.\n"
        msg += "Using fallback..."
        if self.args.get('CMSSWFallback', None) is not None:
            logging.info(msg)
            return self.args['CMSSWFallback']
        else:
            msg += " No CMSSWFallback setting provided."
            raise RuntimeError, msg

    if len(fileList) == 0:
        msg = "No files in Dataset %s\n for run %s\n" % (datasetName, run)
        msg += "Cannot extract CMSSW Version from DBS.\n"
        msg += "Using fallback..."
        if self.args.get('CMSSWFallback', None) is not None:
            logging.info(msg)
            return self.args['CMSSWFallback']
        else:
            msg += " No CMSSWFallback setting provided."
            raise RuntimeError, msg
    lastFile = fileList[-1]

    algoList = lastFile['AlgoList']
    if len(algoList) == 0:
        msg = "No algorithm information in Dataset %si\n for run %s\n" % (
            datasetName, run)
        msg += "Cannot extract CMSSW Version from DBS.\n"
        msg += "Using fallback..."
        if self.args.get('CMSSWFallback', None) is not None:
            logging.info(msg)
            return self.args['CMSSWFallback']
        else:
            msg += " No CMSSWFallback setting provided."
            raise RuntimeError, msg
    lastAlgo = lastFile['AlgoList'][-1]

    return lastAlgo['ApplicationVersion']


def findGlobalTagForDataset(dbsUrl, primary, processed, tier):
    """
    _findGlobalTagForDataset_

    Look up the global tag for a Dataset in DBS. It might not work 100% of the
    times. In that case, it would be possible to use GlobalTagFallback
    parameter.

    This methos relies on the fact that RelVals, MC and ReProcessing have a
    single GlobalTag across run boundaries.

    It won't work for Harversting Tier-0 spat workflows.

    """
    reader  = DBSReader(dbsUrl)
    datasetName = "/%s/%s/%s" % (primary, processed, tier)

    try:
        procDSList = \
            reader.dbs.listProcessedDatasets(primary, tier, processed)
    except Exception, ex:
        msg = "Failed to get details from DBS for dataset:\n"
        msg += "%s\n" % (datasetName)
        msg += "Cannot extract Global Tag from DBS"
        raise RuntimeError, msg

    if len(procDSList) == 0:
        msg = "No Processed Dataset for Dataset %s\n" % (datasetName)
        msg += "Cannot extract Global Tag from DBS"
        raise RuntimeError, msg
    lastprocDS = procDSList[-1]

    globalTag = lastprocDS.get('GlobalTag', None)
    if globalTag is None:
        msg = "Failed to get Global Tag from DBS.\n"
        if self.args.get('GlobalTagFallback', None) is not None:
            globalTag = self.args['GlobalTagFallback']
            msg += "Using Global Tag fallback: %s" % globalTag
            logging.info(msg)
        else:
            msg += "No Global Tag fallback provided in the configuration "
            msg += "file. Can't process input dataset: %s" % datasetName
            raise RuntimeError, msg
    else:
        msg = "Global Tag found in DBS: %s" % globalTag
        logging.info(msg)

    return globalTag


def getLFNForDataset(dbsUrl, primary, processed, tier):
    """
    _findGlobalTagForDataset_

    Look up the LFN's for a Dataset in DBS

    """
    reader  = DBSReader(dbsUrl)
    datasetName = "/%s/%s/%s" % (primary, processed, tier)

    try:
        fileList = reader.dbs.listFiles(path = datasetName)
    except Exception, ex:
        msg = "Failed to get details from DBS for dataset:\n"
        msg += "%s\n" % (datasetName)
        msg += "Cannot extract Global Tag from DBS"
        raise RuntimeError, msg

    if len(fileList) == 0:
        msg = "No files in Dataset %s\n" % (datasetName)
        msg += "Cannot extract Global Tag from DBS"
        raise RuntimeError, msg

    return [x['LogicalFileName'] for x in fileList]



class RelValPlugin(BasePlugin):
    """
    _RelValPlugin_

    Plugin for looking up a RelVal dataset's information and produce a DQM
    Harvesting job for it.

    This plugin actually could be used for any MC dataset in which run number
    is 1.

    """
    def __init__(self):
        BasePlugin.__init__(self)
        # Do we wan to keep the dbsurl fixed?
        self.dbsUrl = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"


    def __call__(self, collectPayload):
        """
        _operator(collectPayload)_

        Given the dataset in the payload, callout to DBS
        to find the files to be harvested

        """
        msg = "RelValPlugin invoked for %s" % str(collectPayload)
        logging.info(msg)

        if collectPayload.get('Scenario', None) is None:
            msg = "RelValPlugin: Payload should provide a scenario."
            raise RuntimeError, msg    

        site = self.args.get("Site", "srm-cms.cern.ch")

        baseCache = os.path.join(self.args['ComponentDir'],
                                 "RelValPlugin")
        if not os.path.exists(baseCache):
            os.makedirs(baseCache)

        datasetCache = os.path.join(baseCache,
                                    collectPayload['PrimaryDataset'],
                                    collectPayload['ProcessedDataset'],
                                    collectPayload['DataTier'])

        if not os.path.exists(datasetCache):
            os.makedirs(datasetCache)

        workflowFile = os.path.join(
            datasetCache,
            "%s-%s-%s-DQMHarvest-Workflow.xml" % (
            collectPayload['PrimaryDataset'],
            collectPayload['ProcessedDataset'],
            collectPayload['DataTier'])
            )

        if not os.path.exists(workflowFile):
            msg = "No workflow found for dataset: %s\n " % (
                collectPayload.datasetPath())
            msg += "Looking up software version and generating workflow..."
            logging.info(msg)

            # Override Global Tag?
            if self.args.get("OverrideGlobalTag", None) is not None:
                globalTag = self.args['OverrideGlobalTag']
                msg = "Using Overrride for Global: %s" % globalTag
                logging.info(msg)
            # Global Tag provided in the payload?
            elif collectPayload.get('GlobalTag', None) is not None:
                globalTag = collectPayload['GlobalTag']
                msg = "Global tag found in payload: %s" % globalTag
                logging.info(msg)
            # Look up in DBS for Global Tag, use fallback GT as last resort
            else:
                globalTag = findGlobalTagForDataset(
                    self.dbsUrl,
                    collectPayload['PrimaryDataset'],
                    collectPayload['ProcessedDataset'],
                    collectPayload['DataTier'])

            # Override CMSSW Version
            if self.args.get("OverrideCMSSW", None) is not None:
                cmsswVersion = self.args['OverrideCMSSW']
                msg = "Using Override for CMSSW Version %s" % (
                    self.args['OverrideCMSSW'],)
                logging.info(msg)
            # CMSSW Version provided in the payload?
            elif collectPayload.get('CMSSWVersion', None) is not None:
                cmsswVersion = collectPayload['CMSSWVersion']
                msg = "CMSSW Version found in payload: %s" % cmsswVersion
                logging.info(msg)
            else:
                cmsswVersion = findVersionForDataset(
                    self.dbsUrl,
                    collectPayload['PrimaryDataset'],
                    collectPayload['ProcessedDataset'],
                    collectPayload['DataTier'],
                    collectPayload['RunNumber'])
                msg = "CMSSW Version for dataset/run\n"
                msg += " Dataset %s\n" % collectPayload.datasetPath()
                msg += " CMSSW Version = %s\n " % cmsswVersion
                logging.info(msg)

            workflowSpec = createHarvestingWorkflow(
                collectPayload.datasetPath(),
                site,
                self.args['CmsPath'],
                self.args['ScramArch'],
                cmsswVersion,
                globalTag,
                self.args['ConfigFile'],
                self.args['DQMServer'],
                self.args['proxyLocation'],
                self.args['DQMCopyToCERN'])
            
            workflowSpec.save(workflowFile)
            msg = "Created Harvesting Workflow:\n %s" % workflowFile
            msg += "\nThe following parameters were used:\n"
            msg += "DQMserver     ==> %s\n" % (self.args['DQMServer'])
            msg += "proxyLocation ==> %s\n" % (self.args['proxyLocation'])
            msg += "DQMCopyToCERN ==> %s\n" % (self.args['DQMCopyToCERN'])
            logging.info(msg)
            self.publishWorkflow(workflowFile, workflowSpec.workflowName())
        else:
            msg = "Loading existing workflow for dataset: %s\n " % (
                collectPayload.datasetPath())
            msg += " => %s\n" % workflowFile
            logging.info(msg)

            workflowSpec = WorkflowSpec()
            workflowSpec.load(workflowFile)

        job = {}
        jobSpec = workflowSpec.createJobSpec()
        jobName = "%s-%s-%s" % (
            workflowSpec.workflowName(),
            collectPayload['RunNumber'],
            time.strftime("%H-%M-%S-%d-%m-%y")
            )

        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Processing")
        jobSpec.parameters['RunNumber'] = collectPayload['RunNumber']  # How should we manage the run numbers?
        jobSpec.parameters['Scenario'] = collectPayload['Scenario']
        jobSpec.addWhitelistSite(site)
        jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.cfgInterface.inputFiles.extend(
            getLFNForDataset(self.dbsUrl,
                             collectPayload['PrimaryDataset'],
                             collectPayload['ProcessedDataset'],
                             collectPayload['DataTier']))

        specCacheDir =  os.path.join(
            datasetCache, str(int(collectPayload['RunNumber']) // 1000).zfill(4))
        if not os.path.exists(specCacheDir):
            os.makedirs(specCacheDir)
        jobSpecFile = os.path.join(specCacheDir,
                                   "%s-JobSpec.xml" % jobName)

        jobSpec.save(jobSpecFile)

        job["JobSpecId"] = jobName
        job["JobSpecFile"] = jobSpecFile
        job['JobType'] = "Harvesting"
        job["WorkflowSpecId"] = workflowSpec.workflowName(),
        job["WorkflowPriority"] = 10
        job["Sites"] = [site]
        job["Run"] = collectPayload['RunNumber']
        job['WorkflowSpecFile'] = workflowFile

        msg = "Harvesting Job Created for\n"
        msg += " => Run:       %s\n" % collectPayload['RunNumber']
        msg += " => Primary:   %s\n" % collectPayload['PrimaryDataset']
        msg += " => Processed: %s\n" % collectPayload['ProcessedDataset']
        msg += " => Tier:      %s\n" % collectPayload['DataTier']
        msg += " => Workflow:  %s\n" % job['WorkflowSpecId']
        msg += " => Job:       %s\n" % job['JobSpecId']
        msg += " => Site:      %s\n" % job['Sites']
        logging.info(msg)

        return [job]

