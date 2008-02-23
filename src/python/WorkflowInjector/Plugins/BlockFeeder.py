#!/usr/bin/env python
"""
_BlockFeeder_

Plugin to generate a fixed amount of production jobs from a workflow that
processes a dataset

The input to this plugin is a workflow that contains the following
parameters:

- SplitType     event or file
- SplitSize     number of events or files per job
- InputDataset  List of InputDataset
- DBSURL        URL of DBS Instance containing the datasets


The list of blocks seen is stored, and if the same workflow is injected
again, only new blocks are converted into jobs.

"""

import logging
import os
import pickle


from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from ProdCommon.JobFactory.DatasetJobFactory import DatasetJobFactory
from ProdAgentCore.Configuration import loadProdAgentConfiguration

from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
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
                                                                                                                                 
    return dbsConfig.get("DBSURL", None)


class PersistencyFile:
    """
    Store last run used and list of blocks in an pickle
    file

    """
    def __init__(self):
        self.blocks = []
        self.run = 1

 


class BlockFeeder(PluginInterface):
    """
    _BlockFeeder_

    Generate a pile of processing style jobs based on the workflow
    and dataset provided

    """
    def handleInput(self, payload):
        logging.info("BlockFeeder: Handling %s" % payload)
        self.workflow = None
        self.dbsUrl = None
        self.blocks = []
        self.workflowFile=payload
        self.loadPayloads(self.workflowFile)

        logging.debug("Now making DBS query & constructing jobs") 
        msg = """however feel free to sing along:

        
                 The sun'll come out 
                      Tomorrow
                Bet your bottom dollar 
                   That tomorrow 
                  There'll be sun!
                  
                       Stop!
                    Millertime!

        ...humming SuperFreak bass line...
        """
        logging.debug(msg)

        self.importDataset()  
        logging.debug("Dataset imported -- calling makeBlockList...")      
        msg = """

              ...She's a very kinky girl
       The kind you don't take home to mother...

        """ 
        logging.debug(msg)

        self.publishNewDataset(self.workflowFile)

        self.makeBlockList()


        factory = DatasetJobFactory(self.workflow,
                                    self.workingDir,
                                    self.dbsUrl,
                                    InitialRun = self.persistData.run)


        msg = """

             ...She's super-freaky, yow
             Super freak, super freak...

        """
        logging.debug(msg)
        jobs = factory()
        for job in jobs:
            self.queueJob(job['JobSpecId'], job['JobSpecFile'],
                          job['JobType'],
                          job['WorkflowSpecId'],
                          job['WorkflowPriority'],
                          *job['Sites'])
            
        self.persistData.run += len(jobs)
        handle = open(self.persistFile, 'w')
        pickle.dump(self.persistData, handle)
        handle.close()
        
        return
        

    def loadPayloads(self, workflowFile):
        """
        _loadPayloads_
        
        
        """
        self.workflow = self.loadWorkflow(workflowFile)
        cacheDir = os.path.join(
            self.workingDir,
            "%s-Cache" % self.workflow.workflowName())
        if not os.path.exists(cacheDir):
            os.makedirs(cacheDir)
        self.persistFile = os.path.join(
            cacheDir, "State.pkl")
        
        self.persistData = PersistencyFile()

        if os.path.exists(self.persistFile):
            handle = open(self.persistFile, 'r')
            self.persistData = pickle.load(handle)
            handle.close()

        #  //
        # // New workflow?  If so, publish it
        #//
        if self.persistData.run == 1:
            self.publishWorkflow(workflowFile, self.workflow.workflowName())
            
        onlyBlocks = self.workflow.parameters.get("OnlyBlocks", None)
        if onlyBlocks != None:
            msg = "OnlyBlocks setting conflicts with BlockFeeder\n"
            msg += "Logic. You cannot use OnlyBlocks with this plugin"
            raise RuntimeError, msg
        
        value = self.workflow.parameters.get("DBSURL", None)
        if value != None:
            self.dbsUrl = value

        if self.dbsUrl == None:
            self.dbsUrl = getGlobalDBSURL()
            self.workflow.parameters['DBSURL'] = self.dbsUrl
            msg = "No DBSURL in workflow: Switching to global DBS\n"
            logging.info(msg)
            



        return


    def makeBlockList(self):
        """
        _makeBlockList_


        Generate the list of blocks for the workflow.

        1. Get the list of all blocks from the DBS
        2. Compare to list of blocks in persistency file
        3. Set OnlyBlocks parameter to new blocks
        
        """
        reader = DBSReader( getLocalDBSURL())
        dbsBlocks = reader.listFileBlocks(self.inputDataset())
        
        if self.persistData.blocks != []:
            remover = lambda x : x not in self.persistData.blocks
            newBlocks = filter(remover, dbsBlocks)
            
        else:
            newBlocks = dbsBlocks

        if len(newBlocks) == 0:
            msg = "No New Blocks found for dataset\n"
            raise RuntimeError, msg
        
        blockList = str(newBlocks)
        blockList = blockList.replace("[", "")
        blockList = blockList.replace("]", "")
        blockList = blockList.replace("\'", "")
        blockList = blockList.replace("\"", "")
        self.workflow.parameters['OnlyBlocks'] = blockList
        self.persistData.blocks.extend(newBlocks)
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
        

        
registerPlugin(BlockFeeder, BlockFeeder.__name__)



