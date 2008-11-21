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

from ProdAgentCore.Configuration import loadProdAgentConfiguration

from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader


def findVersionForDataset(dbsUrl, primary, processed, tier):
    """
    _findVersionForDataset_


    """
    reader  = DBSReader(dbsUrl)
    datasetName = "/%s/%s/%s" % (primary, processed, tier)


    try:
        fileList = reader.dbs.listFiles(
            path = datasetName,
            retriveList = ['retrive_algo',])
    except Exception, ex:
        msg = "Failed to get details from DBS for dataset:\n"
        msg += "%s\n" % (datasetName)
        msg += "Cannot extract CMSSW Version from DBS"
        raise RuntimeError, msg


    if len(fileList) == 0:
        msg = "No files in Dataset %s\n" % (datasetName)
        msg += "Cannot extract CMSSW Version from DBS"
        raise RuntimeError, msg
    lastFile = fileList[-1]

    algoList = lastFile['AlgoList']
    if len(algoList) == 0:
        msg = "No algorithm information in Dataset %s\n" % (
            datasetName)
        msg += "Cannot extract CMSSW Version from DBS"
        raise RuntimeError, msg
    lastAlgo = lastFile['AlgoList'][-1]
    return lastAlgo['ApplicationVersion']



def findGlobalTagForDataset(dbsUrl, primary, processed, tier):
    """
    _findGlobalTagForDataset_

    Look up the global tag for a DBS Dataset

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
    lastFile = fileList[-1]

    tag = lastFile['LogicalFileName'].split("/")[6].split("_")[0]
    version = lastFile['LogicalFileName'].split("/")[6].split("_")[1]

    GlobalTag = tag + "_" + version + "::All"

    if tag != "IDEAL" and tag != "STARTUP" :
        msg = "Warning: Dataset %s has not IDEAL nor STARTUP Global Tag.\n" % (
            datasetName)
        logging.info(msg)
        GlobalTag = None
        
    return GlobalTag



def getLFNForDataset(dbsUrl, primary, processed, tier):
    """
    _findGlobalTagForDataset_

    Look up the global tag for a DBS Dataset

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

    return [ x['LogicalFileName'] for x in fileList ]



class RelValPlugin(BasePlugin):

    def __init__(self):
        BasePlugin.__init__(self)
        self.dbsUrl = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"

    def __call__(self, collectPayload):
        """
        _operator(collectPayload)_

        Given the dataset and run in the payload, callout to DBS
        to find the files to be harvested

        """
        msg = "RelValPlugin invoked for %s" % str(collectPayload)
        logging.info(msg)

        site = self.args.get("Site", "srm.cern.ch")

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
                collectPayload.datasetPath(),)
            msg += "Looking up software version and generating workflow..."
            logging.info(msg)

#            if self.args.get("OverrideGlobalTag", None) == None:
            if 1 :
                globalTag = findGlobalTagForDataset(
                    self.dbsUrl,
                    collectPayload['PrimaryDataset'],
                    collectPayload['ProcessedDataset'],
                    collectPayload['DataTier'])
                if globalTag == None :
                    msg = "OverrideGlobalTag parameter will be used...\n"
                    if self.args.get("OverrideGlobalTag", None) != None :
                        globalTag = self.args['OverrideGlobalTag']
                        msg += "=> GlobalTag: %s\n" % (globalTag)
                        logging.info(msg)
                    else :
                        msg += "OverrideGlobalTag parameter is not defined in Configuration.\n"
                        raise RuntimeError, msg                        
#            else:
#                globalTag = self.args['OverrideGlobalTag']


            if self.args.get("OverrideCMSSW", None) != None:
                cmsswVersion = self.args['OverrideCMSSW']
                msg = "Using Override for CMSSW Version %s" % (
                    self.args['OverrideCMSSW'],)
                logging.info(msg)
            else:
                cmsswVersion = findVersionForDataset(
                    self.dbsUrl,
                    collectPayload['PrimaryDataset'],
                    collectPayload['ProcessedDataset'],
                    collectPayload['DataTier'])
                msg = "Found CMSSW Version for dataset/run\n"
                msg += " Dataset %s Run %s\n" % (collectPayload.datasetPath(),
                                                 collectPayload['RunNumber'])
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
                collectPayload.datasetPath(),)
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
        jobSpec.addWhitelistSite(site)
        jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.cfgInterface.inputFiles.extend(getLFNForDataset(self.dbsUrl,collectPayload['PrimaryDataset'],collectPayload['ProcessedDataset'],collectPayload['DataTier']))

        specCacheDir =  os.path.join(
            datasetCache, str(int(collectPayload['RunNumber']) // 1000).zfill(4))
        if not os.path.exists(specCacheDir):
            os.makedirs(specCacheDir)
        jobSpecFile = os.path.join(specCacheDir,
                                   "%s-JobSpec.xml" % jobName)

        jobSpec.save(jobSpecFile)


        job["JobSpecId"] = jobName
        job["JobSpecFile"] = jobSpecFile
        job['JobType'] = "Processing"
        job["WorkflowSpecId"] = workflowSpec.workflowName(),
        job["WorkflowPriority"] = 10
        job["Sites"] = [site]
#        job["Run"] = collectPayload['RunNumber']
        job['WorkflowSpecFile'] = workflowFile

        msg = "Harvesting Job Created for\n"
#        msg += " => Run:       %s\n" % collectPayload['RunNumber']
        msg += " => Primary:   %s\n" % collectPayload['PrimaryDataset']
        msg += " => Processed: %s\n" % collectPayload['ProcessedDataset']
        msg += " => Tier:      %s\n" % collectPayload['DataTier']
        msg += " => Workflow:  %s\n" % job['WorkflowSpecId']
        msg += " => Job:       %s\n" % job['JobSpecId']
        msg += " => Site:      %s\n" % job['Sites']
        logging.info(msg)

        return [job]

