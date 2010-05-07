#!/usr/bin/env python
"""
_RecoveryFeeder_

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

from ProdCommon.JobFactory.RecoveryJobFactory import RecoveryJobFactory
from ProdAgentCore.Configuration import loadProdAgentConfiguration

from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter

from JobQueue.JobQueueDB import JobQueueDB

from MergeSensor.MergeSensorDB.Interface.MergeSensorDB import MergeSensorDB


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
        self.blocks = {}
        self.run = 1

    def getFiles(self):
        files = []
        for value in self.blocks.values():
            files.extend(value)
        return files

    def update(self, blockDict):
        for block in blockDict:
            self.blocks.setdefault(block, []).extend(blockDict[block])


class RecoveryFeeder(PluginInterface):
    """
    _RecoveryFeeder_

    Generate a pile of processing style jobs based on the workflow
    and dataset provided

    """
    def handleInput(self, payload):
        logging.info("RecoveryFeeder: Handling %s" % payload)
        self.workflow = None
        self.dbsUrl = None
        self.blocks = []
        self.sites = None
        self.workflowFile = payload
        self.onlyClosedBlocks = False
        self.providedOnlyBlocks = None
        self.providedOnlyFiles = None
        self.loadPayloads(self.workflowFile)

        msg = "\n======================================================"
        msg += "\nImporting input dataset along with his beloved mother:"
        msg += "\n======================================================"
        logging.info(msg)

        self.importDataset()

        msg = "\nDataset importing completed. Now I'm publishing NewDataset..."
        logging.info(msg)
        
        self.publishNewDataset(self.workflowFile)

        msg = "\nDataset published. Now I'm invoking makeFileList..."
        logging.info(msg)

        self.makeFileList(self.onlyClosedBlocks, sites=self.sites, 
            providedOnlyBlocks=self.providedOnlyBlocks,
            providedOnlyFiles=self.providedOnlyFiles)

        msg = "Processing the following list of files:\n ==> %s" % \
            "\n ==> ".join(self.workflow.parameters['OnlyFiles'].split(','))
        logging.debug(msg)

        factory = RecoveryJobFactory(self.workflow,
                                   self.workingDir,
                                   self.dbsUrl,
                                   InitialRun = self.persistData.run)
        logging.info('Creating jobSpecs...')
        jobs = factory()

        logging.info('Queueing jobs...')
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
        # // Verifying that output datasets are not being watched by another
        #// workflow.
        #\\
        self.checkForWatchedDatasets()

        #  //
        # // New workflow?  If so, publish it
        #//
        if self.persistData.run == 1:
            logging.debug("Hey, I haven't seen this workflow before.")
            self.publishWorkflow(workflowFile, self.workflow.workflowName())

        #  //
        # // OnlyBlocks setting
        #//
        self.providedOnlyBlocks = \
            self.workflow.parameters.get("OnlyBlocks", None)
        if self.providedOnlyBlocks != None:
            msg = "OnlyBlocks setting will be processed."
            msg += "\n\nI will process the intersection of the OnlyBlocks list"
            msg += " and the all new blocks I can find for Dataset.\n"
            logging.info(msg)

        #  //
        # // OnlyFiles setting
        #//
        self.providedOnlyFiles = self.workflow.parameters.get("OnlyFiles", None)
        if self.providedOnlyFiles != None:
            msg = "OnlyFiles setting will be processed."
            msg += "\n\nATTENTION: I will ignore the OnlyBlocks list."
            msg += "\n\nI will process the intersection of the OnlyFiles list"
            msg += " and the all new files I can find for Dataset.\n"
            logging.info(msg)
        else:
            msg = "ATTENTION: I will process every file in the OnlyBlocks "
            msg += "list."
            logging.info(msg)

        #  //
        # // Only closed blocks are long to be processed?
        #//
        onlyClosedBlocks = self.workflow.parameters.get("OnlyClosedBlocks", False)
        if onlyClosedBlocks and onlyClosedBlocks.lower() == "true":
            self.onlyClosedBlocks = True
            msg = "Only closed blocks will be processed."
            logging.info(msg)

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


    def makeFileList(self, onlyClosedBlocks = False, sites=None,
        providedOnlyBlocks=None, providedOnlyFiles=None):
        """
        _makeFileList_


        Generate the list of blocks for the workflow.

        1. Get the list of all blocks from the DBS
        2. Compare to list of blocks in persistency file
        3. Obtain the intersection of the new blocks and the providedOnlyBlocks
           list.
        4. Set OnlyBlocks parameter to intersection obtained.
        
        """
        #reader = DBSReader(self.dbsUrl)
        # At this point, blocks should be in local DBS
        localDBS = getLocalDBSURL()
        reader = DBSReader(localDBS)

        #  //
        # // Querying list of blocks from DBS
        #//
        msg = "Querying for closed blocks in Local DBS: %s ..." % localDBS
        logging.info(msg)
        dbsBlocks = reader.listFileBlocks(self.inputDataset(),
                                            onlyClosedBlocks)
        msg = "Retrieved %s close blocks from Local DBS" % len(dbsBlocks)
        logging.info(msg)

        #  //
        # // Constructing mapping structures block-file
        #//
        filesToBlocks = {}
        blocksToFiles = {}
        dbsFiles = reader.dbs.listFiles(path=self.inputDataset())
        for dbsfile in dbsFiles:
            if dbsfile['Block']['Name'] in dbsBlocks:
                filesToBlocks[dbsfile['LogicalFileName']] = \
                                                    dbsfile['Block']['Name']
                blocksToFiles.setdefault(dbsfile['Block']['Name'], []
                                         ).append(dbsfile['LogicalFileName'])

        # OnlyFiles?
        if providedOnlyFiles is not None and \
            providedOnlyFiles.strip().lower() != 'auto':
            msg = "Using OnlyFiles list:"
            msg += " %s files." % len(providedOnlyFiles.split(','))
            logging.info(msg)
            onlyFiles = [x.strip() for x in providedOnlyFiles.split(',') if x]
        # OnlyFiles=auto
        elif providedOnlyFiles is not None:
            msg = "Automatically generating OnlyFiles list from DBS..."
            logging.info(msg)
            onlyFiles = self.createOnlyFilesFromWorkflow()
        # OnlyBlocks
        elif providedOnlyBlocks is not None:
            msg = "Using OnlyBLocks list:"
            msg += " %s blocks." % len(providedOnlyBlocks.split(','))
            logging.info(msg)
            onlyFiles = []
            for block in \
                    [x.strip() for x in providedOnlyBlocks.split(',') if x]:
                onlyFiles.extend(blocksToFiles[dbsBlocks])
        # Processing everything in DBS
        else:
            msg = "Processing whole input dataset..."
            logging.info(msg)
            onlyFiles = []
            for block in dbsBlocks:
                onlyFiles.extend(blocksToFiles[dbsBlocks])

        if not onlyFiles:
            msg = "No files were found for the input dataset: " + \
                self.inputDataset()
            raise RuntimeError, msg

        #  //
        # // Filter files that were already processed
        #//
        if self.persistData.blocks:
            msg = "Filtering files that were already processed for this"
            msg += " workflow..."
            logging.info(msg)
            processedFiles = self.persistData.getFiles()
            msg = "Persistency file has %s file(s)" % len(processedFiles)
            logging.info(msg)
            remover  = lambda x: x not in processedFiles
            onlyFiles = filter(remover, onlyFiles)
            msg = "%s file(s) were removed" % \
                                    str(len(processedFiles) - len(onlyFiles))
            logging.info(msg)

        if not onlyFiles:
            msg = "No New files were found for the input dataset: " + \
                self.inputDataset()
            raise RuntimeError, msg

        #  //
        # // Filter files in blocks without site info
        #//
        msg = "Filtering blocks according to Site information..."
        logging.info(msg)
        candidateBlocks = {}
        for file in onlyFiles:
            candidateBlocks.setdefault(filesToBlocks[file], []).append(file)
        blocksAtSites = []
        for block in candidateBlocks:
            locations = reader.listFileBlockLocation(block)
            if not locations:
                msg = "Excluding block without site info ==> %s" % block
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
                    msg = "Excluding block without replicas"
                    msg += " in %s ==> %s" % (block, ", ".join(sites))
                    logging.info(msg)
            else:   
                blocksAtSites.append(block)
        if len(blocksAtSites) == 0:
            msg = "No block has site information."
            raise RuntimeError, msg

        #  //
        # // Constructing OnlyBlocks and OnlyFiles list
        #//
        onlyBlocks = {}
        for block in blocksAtSites:
            onlyBlocks[block] = candidateBlocks[block]
        onlyFiles = []
        for block in onlyBlocks:
            onlyFiles.extend(onlyBlocks[block])

        msg = "\n ==> Files to process: %s" % len(onlyFiles)
        msg += "\n ==> Blocks to process: %s" % len(onlyBlocks)
        logging.info(msg)
    
        blockList = ",".join(onlyBlocks.keys())
        fileList = ",".join(onlyFiles)
        self.workflow.parameters['OnlyBlocks'] = blockList
        self.workflow.parameters['OnlyFiles'] = fileList
        self.persistData.update(onlyBlocks)
        return


    def createOnlyFilesFromWorkflow(self):
        """
        _createOnlyFilesFromWorkflow_

        1. Get output datasets produced by the first step. This procedure wont
           work for workflows whose first step does not stage out (For the 
           moment).
        2. Look up parent files in local DBS for the list created in (1).
        3. Look up all the files in input DBS for input dataset.
        4. Files to Process = (3) - (2)
        """
        msg = "Looking for files in the input dataset with no childs in the"
        msg += " output dataset."
        logging.info(msg)
        topNode = self.workflow.payload
        outputDatasets = self.firstStepOutputDatasets(topNode)
        outputDatasets = [x.replace('-unmerged', '') for x in outputDatasets]

        localReader = DBSReader(getLocalDBSURL())
        knownParents = {}
        for dataset in outputDatasets:
            files = localReader.dbs.listFiles(path=dataset,
                                              retriveList=['retrive_parent'])
            for file in files:
                for parentFile in file['ParentList']:
                    knownParents[parentFile['LogicalFileName']] = \
                                                    file['LogicalFileName']

        msg = "First step's output dataset(s): %s" % " ".join(outputDatasets)
        logging.info(msg)
        msg = "%s file(s) has children in the output dataset(s)." % \
                                                            len(knownParents)
        logging.info(msg)

        inputFiles = [x['LogicalFileName'] for x in \
                        localReader.dbs.listFiles(path=self.inputDataset())]

        msg = "%s file(s) has the input dataset %s in total" % (
                                                            len(inputFiles),
                                                             self.inputDataset())
        logging.info(msg)

        filesToProcess = list(set(inputFiles).difference(set(knownParents)))
        msg = "%s file(s) can be processed." % len(filesToProcess)
        logging.info(msg)

        return filesToProcess


    def firstStepOutputDatasets(self, node):
        """
        _firstStepOutputDatasets_

        Recursively look for the first node that stages out datasets.

        """
        try:
            datasets = node._OutputDatasets
        except StandardError, ex:
            msg = "Error extracting output dataset from Workflow's first step:"
            msg += "payload node:/n"
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg

        # This node has output datasets. Done.
        if datasets:
            return [x.name() for x in datasets]

        # No children?
        if not node.children:
            return []

        # This node has children. Return its children's datasets
        outputDatasets = []
        for child in node.children:
            outputDatasets.extend(self.firstStepOutputDatasets(child))

        return outputDatasets


    def outputDatasets(self):
        """
        _outputDatasets_

        Util to extract all the output datasets form the payload.

        """
        outputDatasetList = []
        for dataset in self.workflow.outputDatasets():
            outputDatasetList.append(dataset.name())
        return outputDatasetList


    def checkForWatchedDatasets(self):
        """
        _checkForWatchedDatasets_

        Checks if datasets are being watched by a different Workflow, if so it
        will raise a Runtime exception.

        """
        mergeDatabase = MergeSensorDB()
        watchedDatasets = mergeDatabase.getDatasetList()
        watched = []
        for dataset in self.outputDatasets():
            if dataset in watchedDatasets:
                watched.append(dataset)

        if watched:
            msg = "The following datasets are being watched right now:"
            for dataset in watched:
                msg += "\n ==> %s" % dataset
            msg += "\nVerfiying they belong to the same workflow you've injected:"
            logging.info(msg)
            # Checking if this is a reinjection.
            datasetsForWorkflow = mergeDatabase.getDatasetListFromWorkflow(
                                                self.workflow.workflowName())
            msg = "Sorry, these datasets are related to another workflow, "
            msg += "please close them and try again:"
            # This workflow has no datasets associated
            if not datasetsForWorkflow:
                for dataset in watched:
                    msg += "\n ==> %s" % dataset
                logging.error(msg)
                raise RuntimeError, msg
            # Still here, let's check is the workflows are the same
            check = lambda x: x not in datasetsForWorkflow
            missingDatasets = filter(check, watched)
            if missingDatasets:
                for dataset in missingDatasets:
                    msg += "\n ==> %s" % dataset
                logging.error(msg)
                raise RuntimeError, msg
            # If I get here, everything is fine
            msg = "Datasets are associated to current workflow. "
            msg += "I will continue normally."
            logging.info(msg)
        else:
            msg = "The following datasets will be produced:"
            for dataset in self.outputDatasets():
                msg += "\n ==> %s" % dataset
            logging.info(msg)


    def inputDataset(self):
        """
        _inputDataset_

        Util to get input dataset name

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


registerPlugin(RecoveryFeeder, RecoveryFeeder.__name__)



