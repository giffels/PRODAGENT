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

from JobQueue.JobQueueDB import JobQueueDB



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
        self.onlyClosedBlocks = False
        self.sites = None
        self.providedOnlyBlocks = None
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

        self.makeBlockList(self.onlyClosedBlocks, self.sites,
            self.providedOnlyBlocks)


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
        # // Validating OnlySites List
        #//
        missingSitesList = self.missingSites()
        if missingSitesList:
            msg = "Error: Site(s) "
            msg += "%s" % ", ".join(missingSitesList)
            msg += " in OnlySites restriction is (are) not present in Resource"
            msg += " Control DB."
            raise RuntimeError, msg

        #  //
        # // New workflow?  If so, publish it
        #//
        if self.persistData.run == 1:
            self.publishWorkflow(workflowFile, self.workflow.workflowName())

        #  //
        # // OnlyBlocks? Yes, I will handle them.
        #//
        self.providedOnlyBlocks = self.workflow.parameters.get("OnlyBlocks", None)
        if self.providedOnlyBlocks != None:
            msg = "OnlyBlocks setting will be processed."
            logging.debug(msg)
            msg = """Remember: Never snowboard without a helmet!
            \rI can't get that song out my head...

            ...She's all right, she's all right
            That girl's all right with me, yeah
             She's a super freak, super freak
                She's super-freaky, yow...
            """
            msg += "\nI will process the intersection of the OnlyBlocks list"
            msg += " and the all new blocks I can find for Dataset.\n"
            logging.debug(msg)
        
        onlyClosedBlocks = self.workflow.parameters.get("OnlyClosedBlocks", False)
        if onlyClosedBlocks and onlyClosedBlocks.lower() == "true":
            self.onlyClosedBlocks = True
            msg = "Only closed blocks will be processed."
            logging.info(msg)
        
        siteRestriction = self.workflow.parameters.get("OnlySites", None)
        if siteRestriction != None:
            #  //
            # // restriction on sites present, populate allowedSites list
            #//
            self.sites = []
            msg = "Site restriction provided in Workflow Spec:\n"
            msg += "%s\n" % siteRestriction
            logging.info(msg)
            siteList = siteRestriction.split(",")
            for site in siteList:
                if len(site.strip()) > 0:
                    self.sites.append(site.strip())
        
        value = self.workflow.parameters.get("DBSURL", None)
        if value != None:
            self.dbsUrl = value

        if self.dbsUrl == None:
            self.dbsUrl = getGlobalDBSURL()
            self.workflow.parameters['DBSURL'] = self.dbsUrl
            msg = "No DBSURL in workflow: Switching to global DBS\n"
            logging.info(msg)
            



        return


    def makeBlockList(self, onlyClosedBlocks = False, sites=None, 
        providedOnlyBlocks=None):
        """
        _makeBlockList_


        Generate the list of blocks for the workflow.

        1. Get the list of all blocks from the DBS
        2. Compare to list of blocks in persistency file
        3. Obtain the intersection of the new blocks and the providedOnlyBlocks list.
        4. Set OnlyBlocks parameter to intersection obtained.
        
        """
        reader = DBSReader( getLocalDBSURL())
        dbsBlocks = reader.listFileBlocks(self.inputDataset(), onlyClosedBlocks)
        
        if self.persistData.blocks != []:
            remover = lambda x : x not in self.persistData.blocks
            newBlocks = filter(remover, dbsBlocks)
            
        else:
            newBlocks = dbsBlocks

        #  //
        # // Skipping blocks without site info
        #//
        msg = "Filtering blocks according to Site information..."
        logging.info(msg)
        blocksAtSites = []
        for block in newBlocks:
            locations = reader.listFileBlockLocation(block)
            if not locations:
                msg = "\nSkipping block: "
                msg += "No site info available for block %s " % block
                logging.info(msg)
            elif sites is not None:
                locationInSites = False
                for location in locations:
                    if location in sites:
                        locationInSites = True
                        break
                if locationInSites:
                    blocksAtSites.append(block)
                else:
                    msg = "\nSkipping block: "
                    msg += "Block %s has no replicas in %s" % (block,
                        ", ".join(sites))
                    logging.info(msg)
            else:
                blocksAtSites.append(block)
        newBlocks = blocksAtSites
        
        if len(newBlocks) == 0:
            msg = "No New Blocks found for dataset\n"
            raise RuntimeError, msg
        
        #  //
        # // Check presence of provided Blocks in newBlocks
        #//
        blocksToProcess = []
        if providedOnlyBlocks is not None :
            providedOnlyBlocksList = providedOnlyBlocks.split(',')
            msg = "OnlyBlocks setting provided. Processing it..."
            logging.info(msg)
            msg = "OnlyBlocks list contains %s Blocks." % (
                len(providedOnlyBlocksList))
            logging.info(msg)
            blockCount = 1
            for block in providedOnlyBlocksList :
                if block.strip() in newBlocks :
                    blocksToProcess.append(block.strip())
                    msg = "Block %s: Adding Block %s" % (
                        blockCount, block)
                    msg += " to the Whitelist"
                    logging.info(msg)
                else:
                    msg = "Block %s: Skipping Block %s " % (
                        blockCount, block)
                    msg += "It's no New or it has been processed"
                    msg += " already."
                    logging.info(msg)
                blockCount += 1
        else : 
            blocksToProcess = newBlocks
            msg = "OnlyBlocks setting not provided. Processing"
            msg += " all New Blocks for Dataset\n"
            logging.info(msg)

        if len(blocksToProcess) == 0 :
            msg = "OnlyBlocks list does not match any New Blocks"
            msg += " found for Dataset\n"
            raise RuntimeError, msg

        blockList = str(blocksToProcess)
        blockList = blockList.replace("[", "")
        blockList = blockList.replace("]", "")
        blockList = blockList.replace("\'", "")
        blockList = blockList.replace("\"", "")
        self.workflow.parameters['OnlyBlocks'] = blockList
        self.persistData.blocks.extend(blocksToProcess)
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


    def missingSites(self):
        """
        This method will return:
        - []: if all the sites in the OnlySites restriction provided in the
                the workflow are in the ResourceControlDB. If the OnlySites
                list is empty, it will return True.
        - [sites,not,found]: if any of the sites provided in the OnlySites 
                 restriction is not in the ResourceControlDB
        """
        onlySites = self.workflow.parameters.get("OnlySites", None)

        # The list is empty, exiting.
        if onlySites in (None, "none", "None", ""):
            return []

        # Verifying sites
        jobQueueDB = JobQueueDB()
        missingSites = []
        for site in onlySites.split(","):
            if site.strip() and \
                not jobQueueDB.getSiteIndex(site.strip()):
                missingSites.append(site)
        return missingSites


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
                onlyClosed=self.onlyClosedBlocks,
                skipNoSiteError=True
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



