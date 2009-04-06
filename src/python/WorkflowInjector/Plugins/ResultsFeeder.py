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

__revision__ = "$Id: ResultsFeeder.py,v 1.3 2009/03/27 18:53:56 ewv Exp $"
__version__  = "$Revision: 1.3 $"
__author__   = "ewv@fnal.gov"

import logging
import os

from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from ProdCommon.JobFactory.MergeJobFactory import MergeJobFactory
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.WorkflowTools import addStageOutNode
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSReader

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

        self.dataTier    = 'USER'

        self.workflow = WorkflowSpec()
        self.workflowName = "StoreResults-%s-%s-%s" % \
            (self.cmsswRelease, self.primaryDataset, self.processedDataset)
        self.workflow.setWorkflowName(self.workflowName)
        self.workflow.parameters["WorkflowType"] = "Merge"
        self.workflow.parameters["DataTier"] = self.dataTier
        self.workflow.parameters['DBSURL'] = self.inputDBSURL
        self.inputDatasetName = self.workflow.payload.addInputDataset(
            self.primaryDataset, self.processedDataset
            )
        self.inputDatasetName.update({
            "DataTier":"USER",
            })
        try:
            reader = DBSReader(self.inputDBSURL)
            primaries = reader.dbs.listPrimaryDatasets(self.primaryDataset)
            self.dataType = primaries[0]['Type']
        except:
            self.dataType = 'mc'

#         self.workflow.setRequestCategory("data")
#         self.workflow.setRequestTimestamp(self.timestamp)
#         self.workflow.parameters["ProdRequestID"] = self.run
#         self.workflow.parameters["RunNumber"] = self.run

#         self.workflow.parameters["CMSSWVersion"] = self.cmssw["CMSSWVersion"]
#         self.workflow.parameters["ScramArch"] = self.cmssw["ScramArch"]
#         self.workflow.parameters["CMSPath"] = self.cmssw["CMSPath"]
#
#         if self.useLazyDownload == True:
#             self.workflow.parameters["UseLazyDownload"] = "True"
#         else:
#             self.workflow.parameters["UseLazyDownload"] = "False"
#
        self.cmsRunNode = self.workflow.payload
        self.cmsRunNode.name = "cmsRun1"
        self.cmsRunNode.type = "CMSSW"
        self.cmsRunNode.application["Version"] = self.cmsswRelease
        self.cmsRunNode.application["Executable"] = "cmsRun"
        self.cmsRunNode.application["Project"] = "CMSSW"
        self.cmsRunNode.application["Architecture"] = 'SCRAM Version'

        addStageOutNode(self.cmsRunNode,"stageOut1")
#
#         preExecScript = self.cmsRunNode.scriptControls["PreExe"]
#         preExecScript.append("T0.Tier0Merger.RuntimeTier0Merger")
#
#         inputDataset = self.cmsRunNode.addInputDataset(self.primaryDataset,
#                                                        self.processedDataset)
#         inputDataset["DataTier"] = self.dataTier
#
        outputDataset = self.cmsRunNode.addOutputDataset(self.primaryDataset,
                                                         self.outputDataset,
                                                         "Merged")
        outputDataset["DataTier"] = self.dataTier
        outputDataset["ApplicationName"] = "cmsRun"
        outputDataset["ApplicationProject"] = "CMSSW"
        outputDataset["ApplicationVersion"] = self.cmsswRelease
        outputDataset["ApplicationFamily"] = "Merged"
        outputDataset["PhysicsGroup"] = self.physicsGroup
        outputDataset["ParentDataset"] = "/%s/%s/%s" % (self.primaryDataset,
                                                        self.processedDataset,
                                                        self.dataTier)

        self.workflow.payload.cfgInterface = CMSSWConfig()
        cfgInt = self.workflow.payload.cfgInterface
        cfgInt.sourceType = "PoolSource"
        cfgInt.maxEvents["input"] = -1
        cfgInt.configMetadata["name"] = self.workflowName
        cfgInt.configMetadata["version"] = "AutoGenerated"
        cfgInt.configMetadata["annotation"] = "AutoGenerated By StoreResults"

        outputModule = cfgInt.getOutputModule("Merged")
        outputModule["catalog"] = "%s-Catalog.xml" % outputModule["Name"]
        outputModule["primaryDataset"] = self.primaryDataset
        outputModule["processedDataset"] = self.outputDataset
        outputModule["dataTier"] = self.dataTier
#        outputModule["acquisitionEra"] = self.acquisitionEra
#        outputModule["processingVersion"] = self.processingVersion

        outputDataset["LFNBase"] =   '/store/user/ewv/testresults/%s/' % self.physicsGroup
        outputModule["LFNBase"] = outputDataset["LFNBase"]
        self.workflow.parameters["UnmergedLFNBase"] = outputDataset["LFNBase"]
        self.workflow.parameters["MergedLFNBase"] = outputDataset["LFNBase"]
        outputModule["fileName"] = "%s.root" % outputModule["Name"]

        outputModule["logicalFileName"] = os.path.join(
            outputDataset["LFNBase"], "Merged.root")

        value = self.workflow.parameters.get("DBSURL", None)
        if value != None:
            self.dbsUrl = value


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

        logging.debug('Parsing1: %s' % fileString)

        from WMCore.Services.JSONParser import JSONParser
        parser = JSONParser.JSONParser('DummyURL')
        userParams = parser.dictParser(fileString)

        logging.info('User loaded params: %s' % userParams)

        try:
            self.primaryDataset   = userParams['primaryDataset']
            self.processedDataset = userParams['processedDataset']
            self.outputDataset    = userParams['outputDataset']
            self.cmsswRelease     = userParams['cmsswRelease']
            self.inputDBSURL      = userParams['inputDBSURL']
            self.physicsGroup     = userParams['physicsGroup']
        except KeyError:
            raise RuntimeError("Some parameters missing")


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
        self.workflow.save('/home/ewv/testworkflow.xml')
        self.publishWorkflow('/home/ewv/testworkflow.xml', self.workflow.workflowName())
        self.publishNewDataset('/home/ewv/testworkflow.xml')

        adsFactory = MergeJobFactory(
            self.workflow, self.workingDir, self.dbsUrl
            )
        jobs = adsFactory()


    def loadPayloads(self, workflowFile):
        """
        _loadPayloads_


        """
        logging.info("In RF:loadPayloads")
        self.workflow = self.loadWorkflow(workflowFile)

        cacheDir = os.path.join(self.workingDir,
            "%s-Cache" % self.workflow.workflowName())
        if not os.path.exists(cacheDir):
            os.makedirs(cacheDir)

        value = self.workflow.parameters.get("DBSURL", None)
        if value != None:
            print "Setting dbsUrl to ", value
            self.dbsUrl = value

        return


    def inputDataset(self):
        """
        util to get input dataset name

        """
        logging.info("In RF:inputDataset")
        topNode = self.workflow.payload
        try:
            inputDataset = topNode._InputDatasets[-1]
        except StandardError, ex:
            msg = "Error extracting input dataset from Workflow:\n"
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg

        return inputDataset.name()


    def importDataset(self):
        """
        _importDataset_

        Import the Dataset contents and inject it into the DB.
        Import the dataset to be processed into the local DBS

        """

        localDBS = getInputDBSURL()
        dbsWriter = DBSWriter(localDBS)
        globalDBS = self.dbsUrl

        try:
            dbsWriter.importDataset(
                globalDBS,
                self.inputDataset(),
                localDBS,
                True
                )
        except Exception, ex:
            msg = "Error importing dataset to be processed into local DBS\n"
            msg += "Source Dataset: %s\n" % self.inputDataset()
            msg += "Source DBS: %s\n" % globalDBS
            msg += "Destination DBS: %s\n" % localDBS
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg



registerPlugin(ResultsFeeder, ResultsFeeder.__name__)
