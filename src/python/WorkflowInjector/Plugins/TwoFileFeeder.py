#!/usr/bin/env python
"""
_TwoFileFeeder_

Plugin to generate a fixed amount of production jobs from a workflow that
processes a dataset

The input to this plugin is a workflow that contains the following
parameters:

- InputDataset  List of InputDataset
- DBSURL        URL of DBS Instance containing the datasets

Note that split type is one file per job at present
Note the OnlyBlocks parameter can't be used, this could be implemented
somewhere else.

TODO: Provide plugin/hook system to allow for checks on file staging.
Initial thought is that this may be better done as a plugin for the JobQueue
and or ResourceMonitor
to wait for files to stage for a job and then release the job from the
queue.
Would need the job queue to have some way to list the input files needed
for each job.

"""

import logging
import os
import pickle


from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from ProdCommon.JobFactory.ReRecoJobFactory import ReRecoJobFactory
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


class TwoFileFeeder(PluginInterface):
    """
    _TwoFileFeeder_

    Generate a pile of processing style jobs based on the workflow
    and dataset provided

    """
    def handleInput(self, payload):
        logging.info("TwoFileFeeder: Handling %s" % payload)
        self.workflow = None
        self.dbsUrl = None
        self.blocks = []
        self.sites = None
        self.workflowFile = payload
        self.onlyClosedBlocks = False
        self.providedOnlyBlocks = None
        self.loadPayloads(self.workflowFile)

        self.publishNewDataset(self.workflowFile)

        msg = "\n======================================================"
        msg += "\nImporting input dataset along with his beloved mother:"
        msg += "\n======================================================"
        logging.info(msg)

        self.importDataset()

        msg = "\nDataset importing completed. Now I'm invoking makeBlockList..."
        logging.info(msg)

        self.makeBlockList(self.onlyClosedBlocks, sites=self.sites, 
            providedOnlyBlocks=self.providedOnlyBlocks)

        msg = "Processing the following list of blocks: %s" % \
            self.workflow.parameters['OnlyBlocks']
        logging.debug(msg)

        factory = ReRecoJobFactory(self.workflow,
                                   self.workingDir,
                                   self.dbsUrl,
                                   InitialRun = self.persistData.run)

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

        1. Load persistency file.
        2. If OnlySites available in workflow:
         a. Check that all the sites are known by the Resource Control DB.
         b. Create list of target sites.
        3. Load Global DBS from workflow if not present, load it from config.

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

        siteRestriction = self.workflow.parameters.get("OnlySites", None)
        if siteRestriction != None:
            #  //
            # // restriction on sites present, populate allowedSites list
            #//
            self.sites = []
            msg = "Site restriction provided in Workflow Spec:\n"
            msg += "%s\n" % siteRestriction
            logging.info(msg)
            siteList = [x for x in siteRestriction.split(",") if x.strip()]
            for site in siteList:
                if len(site.strip()) > 0:
                    self.sites.append(site.strip())

        #  //
        # // New workflow?  If so, publish it
        #//
        if self.persistData.run == 1:
            logging.debug("Hey, I haven't seen this workflow before.")
            self.publishWorkflow(workflowFile, self.workflow.workflowName())

        #  //
        # // OnlyBlocks setting
        #//
        self.providedOnlyBlocks = self.workflow.parameters.get("OnlyBlocks", None)
        if self.providedOnlyBlocks != None:
            msg = "OnlyBlocks setting will be processed."
            msg += "\nI will process the intersection of the OnlyBlocks list"
            msg += " and the all new blocks I can find for Dataset.\n"
            logging.debug(msg)

        #  //
        # // Only closed blocks are long to be processed?
        #//
        onlyClosedBlocks = self.workflow.parameters.get("OnlyClosedBlocks", False)
        if onlyClosedBlocks and onlyClosedBlocks.lower() == "true":
            self.onlyClosedBlocks = True

        value = self.workflow.parameters.get("DBSURL", None)
        if value != None:
            self.dbsUrl = value

        if self.dbsUrl == None:
            try:
                self.dbsUrl = getGlobalDBSURL() 
                msg = "\n============================"
                msg += "\nDBS URL not provided in the Workflow."
                msg += "\nUsing Global DBS from $PRODAGENT_CONFIG."
                msg += "\n"
                msg += "\nTarget DBSURL = %s\n" % self.dbsUrl
                msg += "\n============================\n"
                logging.info(msg)
            except Exception, ex:
                msg = "\n============================"
                msg += "\nDBS URL not provided in the Workflow."
                msg += "\nGlobal DBS can't be loaded from $PRODAGENT_CONFIG."
                msg += "\nError: %s" % ex
                msg += "\n============================"
                logging.error(msg)
                raise RuntimeError, msg

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
        reader = DBSReader(self.dbsUrl)
        dbsBlocks = reader.listFileBlocks(self.inputDataset(), onlyClosedBlocks)
        
        if self.persistData.blocks != []:
            remover = lambda x : x not in self.persistData.blocks
            newBlocks = filter(remover, dbsBlocks)
        else:
            newBlocks = dbsBlocks
        
        #  //
        # // Check if blocks are present in the sites. The filter them by site.
        #//
        if sites is not None: 
            blocksAtSites = []
            msg = "Filtering blocks using OnlySites restriction..."
            logging.info(msg)
            for block in newBlocks:
                for location in reader.listFileBlockLocation(block):
                    if location in sites:
                        blocksAtSites.append(block)
                        break
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
                    msg = "Block %s: Skiping Block %s " % (
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

        Import the Input Dataset contents and inject it into the DB. The
        
        DBSWriter.importDataset should also import the parent Dataset. The
        parent importing seems to work with DBS_2_0_8

        """
        
        #  //
        # // Getting Local and Global DBS URLs
        #//
        localDBS = getLocalDBSURL()
        dbsWriter = DBSWriter(localDBS)
        globalDBS = self.dbsUrl

        try:
            dbsWriter.importDataset(
                globalDBS,
                self.inputDataset(),
                localDBS,
                onlyClosed = True
                )
        except Exception, ex:
            msg = "Error importing dataset to be processed into local DBS\n"
            msg += "Source Dataset: %s\n" % self.inputDataset()
            msg += "Source DBS: %s\n" % globalDBS
            msg += "Destination DBS: %s\n" % localDBS
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg


registerPlugin(TwoFileFeeder, TwoFileFeeder.__name__)



