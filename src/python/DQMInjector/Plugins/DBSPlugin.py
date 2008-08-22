#!/usr/bin/env python
"""
_DBSPlugin_

Plugin for retrieving files to harvest from DBS

"""

import logging
import os

from DQMInjector.Plugins.BasePlugin import BasePlugin
from DQMInjector.HarvestWorkflow import createHarvestingWorkflow

from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.JobFactory.RunJobFactory import RunJobFactory




def findVersionForDataset(dbsUrl, primary, processed, tier, run):
    """
    _findVersionForDataset_


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
        msg += "%s\n for run %s\n" % (datasetName, run)
        msg += "Cannot extract CMSSW Version from DBS"
        raise RuntimeError, msg


    if len(fileList) == 0:
        msg = "No files in Dataset %s\n for run %s\n" % (datasetName, run)
        msg += "Cannot extract CMSSW Version from DBS"
        raise RuntimeError, msg
    lastFile = fileList[-1]

    algoList = lastFile['AlgoList']
    if len(algoList) == 0:
        msg = "No algorithm information in Dataset %s\n for run %s\n" % (
            datasetName, run)
        msg += "Cannot extract CMSSW Version from DBS"
        raise RuntimeError, msg
    lastAlgo = lastFile['AlgoList'][-1]
    return lastAlgo['ApplicationVersion']





class DBSPlugin(BasePlugin):

    def __init__(self):
        BasePlugin.__init__(self)
        self.dbsUrl = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"

    def __call__(self, collectPayload):
        """
        _operator(collectPayload)_

        Given the dataset and run in the payload, callout to DBS
        to find the files to be harvested

        """
        msg = "DBSPlugin invoked for %s" % str(collectPayload)
        logging.info(msg)


        site = self.args.get("Site", "srm.cern.ch")

        baseCache = os.path.join(self.args['ComponentDir'],
                                 "DBSPlugin")
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

            cmsswVersion = findVersionForDataset(
                self.dbsUrl,
                collectPayload['PrimaryDataset'],
                collectPayload['ProcessedDataset'],
                collectPayload['DataTier'],
                collectPayload['RunNumber'])
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
                self.args['ConfigFile'])
            
            workflowSpec.save(workflowFile)
            msg = "Created Harvesting Workflow:\n %s" % workflowFile
            logging.info(msg)
            self.publishWorkflow(workflowFile, workflowSpec.workflowName())
        else:
            msg = "Loading existing workflow for dataset: %s\n " % (
                collectPayload.datasetPath(),)
            msg += " => %s\n" % workflowFile
            logging.info(msg)

            workflowSpec = WorkflowSpec()
            workflowSpec.load(workflowFile)


        #  //
        # //  Callout to create the harvesting job for the run
        #//
        factory = RunJobFactory(workflowSpec,
                                datasetCache,
                                self.dbsUrl, SiteName = site,
                                FilterRuns = [int(collectPayload['RunNumber'])],
                                )


        jobs = factory()

        for job in jobs:
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

        return jobs

