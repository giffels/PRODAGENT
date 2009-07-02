#!/usr/bin/env python
"""
_ResultsFeeder_

Results feeder

Merges a /store/user dataset into /store/results. Input parameters are

- Input dataset
- Output dataset
- DBS URL where the dataset is stored
- CMSSW version

"""

__revision__ = "$Id: ResultsFeeder.py,v 1.9 2009/05/14 07:11:54 giffels Exp $"
__version__  = "$Revision: 1.9 $"
__author__   = "ewv@fnal.gov"

import logging
import os

from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from ProdCommon.JobFactory.MergeJobFactory import MergeJobFactory
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.WorkflowTools import addStageOutNode
from ProdCommon.MCPayloads.WorkflowTools import addStageOutOverride
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSReader

from WMCore.Services.JSONParser import JSONParser

def getInputDBSURL():
    """
    _getInputDBSURL_

    Return the input URL for DBS

    """
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    try:
        dbsConfig = config.getConfig("LocalDBS")
    except StandardError, ex:
        msg = "Error reading configuration for LocalDBS:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    return dbsConfig.get("DBSURL", None)

class NodeFinder:
    def __init__(self, name):
        self.name = name
        self.result = None
    def __call__(self, node):
        if node.name == self.name:
            self.result = node
        return

class ResultsFeeder(PluginInterface):
    """
    _ResultsFeeder_

    Generate a set of processing jobs to process an ADS

    """
    def createWorkflow(self):
        """
        _createWorkflow_

        Create the workflow out of the user's inputs and static information

        """

        lfnPrefix = '/store/results'
        dataTier  = 'USER'

        # Temporary stuff to override stageout and location
        srmPrefix = 'srm://cmssrm.fnal.gov:8443/srm/managerv2?SFN=/11/'
        lfnPrefix = '/store/user/ewv/testresults'
        self.workflowName = "SR-%s-%s-%s" % \
            (self.cmsswRelease, self.primaryDataset, self.processedDataset)
        self.workflowFile = os.path.join(self.workingDir,
                                         '%s.xml' % self.workflowName)

        # Create Workflow

        if not os.path.exists(self.workingDir):
            os.makedirs(self.workingDir)

        self.workflow = WorkflowSpec()
        self.workflow.setWorkflowName(self.workflowName)
        self.workflow.parameters["WorkflowType"] = "Merge"
        self.workflow.parameters["DataTier"] = dataTier
        self.workflow.parameters['DBSURL'] = self.dbsUrl

        self.inputDatasetName = self.workflow.payload.addInputDataset(
            self.primaryDataset, self.processedDataset
            )
        self.inputDatasetName.update({
            "DataTier" : dataTier,
            })

        # Get info from input dataset

        try:
            reader = DBSReader(self.inputDBSURL)
            primaries = reader.dbs.listPrimaryDatasets(self.primaryDataset)
            self.dataType = primaries[0]['Type']
        except:
            self.dataType = 'mc'

        # Migrate dataset from User's LocalDBS to StoreResults LocalDBS

        writer = DBSWriter(self.dbsUrl)
        dstURL = self.dbsUrl
        srcURL = self.inputDBSURL
        path = "/%s/%s/USER" % (self.primaryDataset, self.processedDataset)
        logging.info("Migrating dataset %s from %s to %s" %
                     (path, srcURL, dstURL))
        writer.dbs.migrateDatasetContents(srcURL, dstURL, path,
                                          '', False, True)

        # Create node for cmsRun

        self.cmsRunNode = self.workflow.payload
        self.cmsRunNode.name = "cmsRun1"
        self.cmsRunNode.type = "CMSSW"
        self.cmsRunNode.application["Version"] = self.cmsswRelease
        self.cmsRunNode.application["Executable"] = "cmsRun"
        self.cmsRunNode.application["Project"] = "CMSSW"
        self.cmsRunNode.application["Architecture"] = 'SCRAM Version'

        # Add a node for stage out

        addStageOutNode(self.cmsRunNode,"stageOut1")
        for node in self.workflow.payload.children:
            if node.type == "StageOut":
                addStageOutOverride(node,
                    command = "srmv2", option = "",
                    seName = "cmssrm.fnal.gov", lfnPrefix = srmPrefix)

        # Create and populate output dataset

        outputDataset = self.cmsRunNode.addOutputDataset(self.primaryDataset,
                                                         self.outputDataset,
                                                         "Merged")
        outputDataset["DataTier"] = dataTier
        outputDataset["NoMerge"] = "True"
        outputDataset["ApplicationName"] = "cmsRun"
        outputDataset["ApplicationProject"] = "CMSSW"
        outputDataset["ApplicationVersion"] = self.cmsswRelease
        outputDataset["ApplicationFamily"] = "Merged"
        outputDataset["LFNBase"] = '%s/%s/' % (lfnPrefix, self.physicsGroup)
        outputDataset["PhysicsGroup"] = self.physicsGroup
        outputDataset["PrimaryDatasetType"] = self.dataType
        outputDataset["ParentDataset"] = "/%s/%s/%s" % (self.primaryDataset,
                                                        self.processedDataset,
                                                        dataTier)

        # Create and populate config description

        self.workflow.payload.cfgInterface = CMSSWConfig()
        cfgInt = self.workflow.payload.cfgInterface
        cfgInt.sourceType = "PoolSource"
        cfgInt.maxEvents["input"] = -1
        cfgInt.configMetadata["name"] = self.workflowName
        cfgInt.configMetadata["version"] = "AutoGenerated"
        cfgInt.configMetadata["annotation"] = "AutoGenerated By StoreResults"

        # Create and populate config output module

        outputModule = cfgInt.getOutputModule("Merged")
        outputModule["catalog"] = "%s-Catalog.xml" % outputModule["Name"]
        outputModule["primaryDataset"] = self.primaryDataset
        outputModule["processedDataset"] = self.outputDataset
        outputModule["dataTier"] = self.dataTier
        outputModule["LFNBase"] = outputDataset["LFNBase"]
        outputModule["fileName"] = "%s.root" % outputModule["Name"]

        outputModule["logicalFileName"] = os.path.join(
            outputDataset["LFNBase"], "Merged.root")

        self.workflow.parameters["UnmergedLFNBase"] = outputDataset["LFNBase"]
        self.workflow.parameters["MergedLFNBase"] = outputDataset["LFNBase"]

    def loadParams(self, paramFile):
        """
        _loadParams_

        Load user parameters from a JSON file

        """
        fileString = ''

        try:
            lines = [line.rstrip() for line in open(paramFile, 'r')]
            fileString = ' '.join(lines)
        except:
            raise RuntimeError("Problem reading file: %s" % paramFile)

        parser = JSONParser.JSONParser()
        userParams = parser.dictParser(fileString)

        logging.debug('User loaded params: %s' % userParams)

        try:
            self.primaryDataset   = userParams['primaryDataset']
            self.processedDataset = userParams['processedDataset']
            self.outputDataset    = userParams['outputDataset']
            self.cmsswRelease     = userParams['cmsswRelease']
            self.inputDBSURL      = userParams['inputDBSURL']
            self.physicsGroup     = userParams['physicsGroup']
        except KeyError:
            raise RuntimeError("Some parameters missing")

        self.dbsUrl = getInputDBSURL()


    def handleInput(self, payload):
        """
        _handleInput_

        Handle an input payload

        """
        self.workflow = None
        self.dbsUrl = None
        self.workflowFile = payload

        self.loadParams(self.workflowFile)
        self.createWorkflow()
        self.workflow.save(self.workflowFile)
        self.publishWorkflow(self.workflowFile, self.workflow.workflowName())
        self.publishNewDataset(self.workflowFile)

        adsFactory = MergeJobFactory(
            self.workflow, self.workingDir, self.dbsUrl
            )
        jobs = adsFactory()

        for job in jobs:
            self.msRef.publish("CreateJob", job['JobSpecFile'])
        self.msRef.commit()



registerPlugin(ResultsFeeder, ResultsFeeder.__name__)
