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

__revision__ = "$Id: ResultsFeeder.py,v 1.23 2009/12/08 14:29:28 giffels Exp $"
__version__  = "$Revision: 1.23 $"
__author__   = "ewv@fnal.gov"

import logging
import os
import traceback

from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from ProdAgentCore.Configuration import loadProdAgentConfiguration

from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.JobFactory.MergeJobFactory import MergeJobFactory
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.WorkflowTools import addStageOutNode, addStageOutOverride

from WMCore.Services.DBS.DBSWriter import DBSReader
from WMCore.Services.DBS.DBSWriter import DBSWriter
from WMCore.Services.JSONParser    import JSONParser
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.UUID          import makeUUID

from dbsApiException import *

def getLocalDBSURLs():
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

    return dbsConfig.get("ReadDBSURL", None), dbsConfig.get("DBSURL", None)


def getGlobalDBSURL():
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    try:
        dbsConfig = config.getConfig("GlobalDBSDLS")
    except StandardError, ex:
        msg = "Error reading configuration for GlobalDBSDLS:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    return dbsConfig.get("ReadDBSURL", None), dbsConfig.get("DBSURL", None)


def getPhedexDSURL():
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    try:
        dsConfig = config.getConfig("PhEDExDataserviceConfig")
    except StandardError, ex:
        msg = "Error reading configuration for PhEDExDataservice:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    return dsConfig.get("DataserviceURL", None)

def getX509Configuration():
    try:
        config = loadProdAgentConfiguration()

    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    try:
        X509Config = config.getConfig("StoreResultsAccountant")

    except StandardError, ex:
        msg = "Error reading configuration for StoreResultsAccoutant:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    return X509Config.get("X509_USER_CERT", None), X509Config.get("X509_USER_KEY", None)

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
        lfnPrefix = self.resultsDir
        self.workflowName = "SR-%s-%s-%s" % \
            (self.cmsswRelease, self.primaryDataset, self.outputDataset)
        self.workflowFile = os.path.join(self.workingDir,
                                         '%s.xml' % self.workflowName)

        # Create Workflow

        if not os.path.exists(self.workingDir):
            os.makedirs(self.workingDir)

        self.workflow = WorkflowSpec()
        self.workflow.setWorkflowName(self.workflowName)
        self.workflow.parameters["WorkflowType"] = "Merge"
        self.workflow.parameters["DataTier"] = self.dataTier
        self.workflow.parameters['DBSURL'] = self.localWriteURL
        self.workflow.parameters['SubscriptionNode'] = self.injectionNode

        self.inputDatasetName = self.workflow.payload.addInputDataset(
            self.primaryDataset, self.processedDataset
            )
        self.inputDatasetName.update({
            "DataTier" : self.dataTier,
            })

        # Get info from input dataset
        try:
            logging.info("Listing dataset %s" %
                     (self.primaryDataset))
            reader = DBSReader(self.inputDBSURL)
            primaries = reader.dbs.listPrimaryDatasets(self.primaryDataset)
            self.dataType = primaries[0]['Type']
        except:
            self.dataType = 'mc'

        #set enviroment to use X509 Authentication
        X509_USER_CERT, X509_USER_KEY = getX509Configuration()

        if X509_USER_CERT!=None:
            os.environ["X509_USER_CERT"] = X509_USER_CERT

        if X509_USER_KEY!=None:
            os.environ["X509_USER_KEY"] = X509_USER_KEY

        msg = "Using following user certificate %s" % os.getenv("X509_USER_CERT")
        logging.debug(msg)
        msg = "Using following user key %s" % os.getenv("X509_USER_KEY")
        logging.debug(msg)

        # Figure out what Phedex node we'll be injecting into

        dict = {'endpoint' : self.phedexURL}
        phedexApi = PhEDEx(dict)

        blockList = reader.dbs.listBlocks(dataset = self.datasetName)
        seHost = blockList[0]['StorageElementList'][0]['Name']

#         seHost = 'srm.test1.ch'
        phedexNodes = phedexApi.getNodeNames(seHost)
        phedexNode = None
        if len(phedexNodes) > 0:
            phedexNode = phedexNodes[0] # By default
            for name in phedexNodes:    # Search for MSS and prefer that
                if name.find('MSS') > -1:
                    phedexNode = name

        logging.info("Data resides on %s" % phedexNode)
        self.workflow.parameters['InjectionNode'] = phedexNode

        logging.debug("Datatype = %s" % self.dataType)

        # Migrate dataset from User's LocalDBS to StoreResults LocalDBS

        skipParents = False
        readWrite = True

        path = "/%s/%s/USER" % (self.primaryDataset, self.processedDataset)

        logging.info("Connecting to DBS writer with URL %s" % self.localWriteURL)
        writer = DBSWriter(self.localWriteURL)
        logging.info("Migrating dataset %s from %s to %s" %
                     (path, self.inputDBSURL, self.localWriteURL))
        try:
            writer.dbs.migrateDatasetContents(self.inputDBSURL,
                        self.localWriteURL, path, '', skipParents, readWrite)
        except:
            logging.info("Migrating to local DBS failed:\n%s" % traceback.format_exc())
            raise RuntimeError("Migrating %s to local DBS failed" % path)

        # Migrate dataset from User's LocalDBS to Global DBS
        logging.info("Connecting to DBS writer with URL %s" % self.globalDbsUrl)

        writer = DBSWriter(self.globalDbsUrl)
        logging.info("Migrating dataset %s from %s to %s" %
                     (path, self.inputDBSURL, self.globalDbsUrl))
        try:
            writer.dbs.migrateDatasetContents(self.inputDBSURL,
                        self.globalDbsUrl, path, '', skipParents, True)
        except:
            logging.info("Migrating to global DBS failed:\n%s" % traceback.format_exc())
            raise RuntimeError("Migrating %s to global DBS failed" % path)

        # Check for existence of target dataset in GlobalDBS
        globalReader = DBSReader(self.globalReadURL)

        exists = globalReader.dbs.listProcessedDatasets(
                    patternPrim = self.primaryDataset,
                    patternProc = self.outputDataset,
                    patternDT   = self.dataTier)
        if exists:
            raise RuntimeError("Dataset %s already exists in global DBS" % self.outputDatasetName)

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

        # Temporary stuff to override stageout and location
        if self.FNALOverride:
            srmPrefix = 'srm://cmssrm.fnal.gov:8443/srm/managerv2?SFN=/11/'
            for node in self.workflow.payload.children:
                if node.type == "StageOut":
                    addStageOutOverride(node,
                        command = "srmv2", option = "",
                        seName = "cmssrm.fnal.gov", lfnPrefix = srmPrefix)

        # Create and populate output dataset

        outputDataset = self.cmsRunNode.addOutputDataset(self.primaryDataset,
                                                         self.outputDataset,
                                                         "Merged")
        lfnBase = '%s/%s/%s/%s/' % (lfnPrefix,
            self.physicsGroup, self.primaryDataset, self.outputDataset)

        outputDataset["DataTier"] = self.dataTier
        outputDataset["NoMerge"] = "True"
        outputDataset["ApplicationName"] = "cmsRun"
        outputDataset["ApplicationProject"] = "CMSSW"
        outputDataset["ApplicationVersion"] = self.cmsswRelease
        outputDataset["ApplicationFamily"] = "Merged"
        outputDataset["LFNBase"] = lfnBase

        outputDataset["PhysicsGroup"] = self.physicsGroup
        outputDataset["PrimaryDatasetType"] = self.dataType
        outputDataset["ParentDataset"]      =  self.datasetName

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
        outputModule["LFNBase"] = lfnBase
        outputModule["fileName"] = "%s.root" % outputModule["Name"]

        outputModule["logicalFileName"] = os.path.join(
            lfnBase, "Merged.root")

        self.workflow.parameters["UnmergedLFNBase"] = lfnBase
        self.workflow.parameters["MergedLFNBase"]   = lfnBase

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

        self.FNALOverride = False
        self.resultsDir = "/store/results"
        self.dataTier  = 'USER'

        try:
            self.primaryDataset   = userParams['primaryDataset']
            self.processedDataset = userParams['processedDataset']
            self.outputDataset    = userParams['outputDataset']
            self.cmsswRelease     = userParams['cmsswRelease']
            self.inputDBSURL      = userParams['inputDBSURL']
            self.physicsGroup     = userParams['physicsGroup']
            self.injectionNode    = userParams['destinationSite']
        except KeyError:
            raise RuntimeError("Some parameters missing")

        self.datasetName = "/%s/%s/%s" % (self.primaryDataset,
                                          self.processedDataset,
                                          self.dataTier)
        self.outputDatasetName = "/%s/%s/%s" % (self.primaryDataset,
                                                self.outputDataset,
                                                self.dataTier)

        if  userParams.get('FNALOverride','False') == 'True':
            self.FNALOverride = True
        self.resultsDir = userParams.get('resultsDir',"/store/results")

        self.localReadURL,  self.localWriteURL = getLocalDBSURLs()
        self.globalReadURL, self.globalDbsUrl  = getGlobalDBSURL()
        self.phedexURL                         = getPhedexDSURL()

    def handleInput(self, payload):
        """
        _handleInput_

        Handle an input payload

        """
        self.workflow      = None
        self.localReadURL  = None
        self.localWriteURL = None
        self.globalReadURL = None
        self.globalDbsUrl  = None

        self.workflowFile = payload

        self.loadParams(self.workflowFile)
        self.createWorkflow()
        self.workflow.save(self.workflowFile)
        self.publishWorkflow(self.workflowFile, self.workflow.workflowName())
        self.publishNewDataset(self.workflowFile)

        jobFactory = MergeJobFactory(
            self.workflow, self.workingDir, self.localReadURL
            )
        jobs = jobFactory()

        for job in jobs:
            self.msRef.publish("CreateJob", job['JobSpecFile'])
        self.msRef.commit()


    def loadWorkflow(self, specFile):
        """
        _loadWorkflow_

        Dummy method to satisfy __call__ of PluginInterface

        """
        spec = WorkflowSpec()
        spec.setWorkflowName(makeUUID())
        return spec




registerPlugin(ResultsFeeder, ResultsFeeder.__name__)
