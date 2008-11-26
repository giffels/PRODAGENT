#!/usr/bin/env python
"""
_ADSFeeder_

Analysis Dataset feeder

Splits an ADS by file using parameters:

- SplitSize
- DBSURL

Input ADS will be extracted from the first node in the workflow

"""


import logging
import os
import pickle


from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from ProdCommon.JobFactory.ADSJobFactory import ADSJobFactory
from ProdAgentCore.Configuration import loadProdAgentConfiguration

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter


def getLocalDBSURL():
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

    return dbsConfig.get("ReadDBSURL", None)



class ADSFeeder(PluginInterface):
    """
    _ADSFeeder_

    Generate a set of processing jobs to process an ADS

    """
    def handleInput(self, payload):
        """
        _handleInput_

        Handle an input payload

        """
        self.workflow = None
        self.dbsUrl = None
        self.workflowFile = payload
        self.loadPayloads(self.workflowFile)



        adsFactory = ADSJobFactory(self.workflow, self.workingDir, self.dbsUrl)

        jobs = adsFactory()



    def loadPayloads(self, workflowFile):
        """
        _loadPayloads_


        """
        self.workflow = self.loadWorkflow(workflowFile)

        ads = self.inputDataset()
        adsSplit = ads.strip("/").split("/")
        if len(adsSplit) != 4:
            msg = "Input Dataset doesnt look like an Analysis Dataset:\n"
            msg += "%s\n" % ads
            msg += "Doesnt contain four pieces"
            raise RuntimeError, msg



        cacheDir = os.path.join(
            self.workingDir,
            "%s-Cache" % self.workflow.workflowName())
        if not os.path.exists(cacheDir):
            os.makedirs(cacheDir)




        #self.publishWorkflow(workflowFile, self.workflow.workflowName())


        value = self.workflow.parameters.get("DBSURL", None)
        if value != None:
            self.dbsUrl = value

        if self.dbsUrl == None:
            self.dbsUrl = getGlobalDBSURL()
            self.workflow.parameters['DBSURL'] = self.dbsUrl
            msg = "No DBSURL in workflow: Switching to global DBS\n"
            logging.info(msg)




        return

    def inputDataset(self):
        """
        util to get input dataset name

        """
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

        """

        #  //
        # // Import the dataset to be processed into the local DBS
        #//
        localDBS = getLocalDBSURL()
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



registerPlugin(ADSFeeder, ADSFeeder.__name__)
