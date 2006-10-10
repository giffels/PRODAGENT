#!/usr/bin/env python
"""
_DatasetIterator_

Maintain a Workflow specification, and when prompted,
generate a new concrete job from that workflow based on a JobDefinition object
defining the input LFNs and event range



"""

import os
import logging


from MCPayloads.WorkflowSpec import WorkflowSpec
from MCPayloads.LFNAlgorithm import createUnmergedLFNs
from CMSConfigTools.CfgGenerator import CfgGenerator

from DatasetInjector.SplitterMaker import createJobSplitter
import DatasetInjector.DatasetInjectorDB as DatabaseAPI


class DatasetIterator:
    """
    _DatasetIterator_

    Working from a Generic Workflow template, generate
    concrete jobs from it, keeping in-memory history

    """
    def __init__(self, workflowSpecFile, workingDir):
        self.workflow = workflowSpecFile
        self.workingDir = workingDir
        self.currentJob = None
        self.workflowSpec = WorkflowSpec()
        self.workflowSpec.load(workflowSpecFile)
        self.currentJobDef = None
        self.count = 0
        self.allowedBlocks = []
        self.allowedSites = []
        self.dbsdlsContact = {}
        self.splitType = \
                self.workflowSpec.parameters.get("SplitType", "file").lower()
        self.splitSize = int(self.workflowSpec.parameters.get("SplitSize", 1))
        
        #  //
        # // Does the workflow contain a block restriction??
        #//
        blockRestriction = \
             self.workflowSpec.parameters.get("OnlyBlocks", None)
        if blockRestriction != None:
            #  //
            # // restriction on blocks present, populate allowedBlocks list
            #//
            msg = "Block restriction provided in Workflow Spec:\n"
            msg += "%s\n" % blockRestriction
            blockList = blockRestriction.split(",")
            for block in blockList:
                if len(block.strip() ) > 0:
                    self.allowedBlocks.append(block.strip())

        #  //
        # // Does the workflow contain a site restriction??
        #//
        siteRestriction = \
           self.workflowSpec.parameters.get("OnlySites", None)          
        if siteRestriction != None:
            #  //
            # // restriction on sites present, populate allowedSites list
            #//
            msg = "Site restriction provided in Workflow Spec:\n"
            msg += "%s\n" % siteRestriction
            siteList = siteRestriction.split(",")
            for site in siteList:
                if len(site.strip() ) > 0:
                    self.allowedSites.append(site.strip())

        #  //
        # // Is the DBS/DLS contact information provided??
        #//
        for item in ['DBSURL', 'DBSAddress', 'DLSType', 'DLSAddress']:
            value = self.workflowSpec.parameters.get(item, None)
            if value != None:
                self.dbsdlsContact[item] = value
        
    def __call__(self, jobDef):
        """
        _operator()_

        When called generate a new concrete job payload from the
        generic workflow and return it.
        The JobDef should be a JobDefinition with the input details
        including LFNs and event ranges etc.

        """
        newJobSpec = self.createJobSpec(jobDef)
        self.count += 1
        return newJobSpec

    def inputDataset(self):
        """
        _inputDataset_

        Extract the input Dataset from this workflow

        """
        topNode = self.workflowSpec.payload
        try:
            inputDataset = topNode._InputDatasets[-1]
        except StandardError, ex:
            msg = "Error extracting input dataset from Workflow:\n"
            msg += str(ex)
            logging.error(msg)
            return None

        return inputDataset.name()
        
            
    def createJobSpec(self, jobDef):
        """
        _createJobSpec_

        Load the WorkflowSpec object and generate a JobSpec from it

        """
        
        jobSpec = self.workflowSpec.createJobSpec()
        jobName = "%s-%s" % (
            self.workflowSpec.workflowName(),
            self.count,
            )
        self.currentJob = jobName
        self.currentJobDef = jobDef
        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Processing")
        jobSpec.parameters['RunNumber'] = self.count

        
        jobSpec.payload.operate(self.generateJobConfig)
        jobSpecFile = os.path.join(self.workingDir,
                                   "%s-JobSpec.xml" % jobName)

        #  //
        # // generate LFNs for output modules
        #//
        createUnmergedLFNs(jobSpec)

        #  //
        # // Add site pref if set
        #//
        for site in jobDef['SENames']:
            jobSpec.addWhitelistSite(site)
            
        
        jobSpec.save(jobSpecFile)
        
        return "file://%s" % jobSpecFile
        
        
    def generateJobConfig(self, jobSpecNode):
        """
        _generateJobConfig_
        
        Operator to act on a JobSpecNode tree to convert the template
        config file into a JobSpecific Config File
                
        """
        if jobSpecNode.configuration in ("", None):
            #  //
            # // Isnt a config file
            #//
            return
        try:
            generator = CfgGenerator(jobSpecNode.configuration, True)
        except StandardError, ex:
            #  //
            # // Cant read config file => not a config file
            #//
            return

        maxEvents = self.currentJobDef.get("MaxEvents", None)
        skipEvents = self.currentJobDef.get("SkipEvents", None)

        args = {
            'fileNames' : self.currentJobDef['LFNS'],
            }
    
        if maxEvents != None:
            args['maxEvents'] = maxEvents
        if skipEvents != None:
            args['skipEvents'] = skipEvents
        
        
        jobCfg = generator(self.currentJob, **args)
        
        jobSpecNode.configuration = jobCfg.cmsConfig.asPythonString()
        jobSpecNode.loadConfiguration()
        
        return
    

    def importDataset(self):
        """
        _importDataset_

        Import the Dataset contents and inject it into the DB.

        """
        try:
            splitter = createJobSplitter(self.inputDataset(),
                                     **self.dbsdlsContact)
        except Exception, ex:
            msg = "Unable to extract details from DBS/DLS for dataset:\n"
            msg += "%s\n" % self.inputDataset()
            msg += str(ex)
            logging.error(msg)
            return 1 

        fileCount = splitter.totalFiles()
        logging.debug("Dataset contains %s files" % fileCount)
        if  fileCount == 0:
            msg = "Dataset Contains no files:\n"
            msg += "%s\n" % self.inputDataset()
            msg += "Unable to inject empty dataset..."
            logging.error(msg)
            return 1

        #  //
        # // Create entry in DB for workflow name
        #//
        try:
            owner = DatabaseAPI.createOwner(self.workflowSpec.workflowName())
        except Exception, ex:
            msg = "Failed to create Entry in DB for Workflow Spec Name:\n"
            msg += "%s\n" % self.workflowSpec.workflowName()
            msg += str(ex)
            logging.error(msg)
            return 1

        return self.insertSplitter(splitter, owner)

    

    def insertSplitter(self, splitter, owner):
        """
        _insertSplitter_

        Insert the contents of the JobSplitter instance provided for the
        owner provided.
        
        """
        #  //
        # // Now insert data into DB
        #//
        logging.debug("SplitSize = %s" % self.splitSize)
        for block in splitter.listFileblocks():
            blockInstance = splitter.fileblocks[block]
            #  //
            # // Check list of allowed blocks
            #//
            if len(self.allowedBlocks) > 0:
                if block not in self.allowedBlocks:
                    msg = "Fileblock not in list of allowed blocks: "
                    msg += "%s\n" % block
                    msg += "This block will not be imported"
                    logging.info(msg)
                    continue
            #  //
            # // Check list of allowed sites
            #//
            if len(self.allowedSites) > 0:
                siteOK = False
                for site in blockInstance.seNames:
                    if site in self.allowedSites:
                        siteOK = True
                if not siteOK:
                    msg = "Fileblock not at allowed site: %s\n" % block
                    msg += "Sites : %s\n" %  blockInstance.seNames
                    msg += "This block will not be imported"
                    logging.info(msg)
                    continue
            
            #  //
            # // Check for empty file blocks
            #//
            if blockInstance.isEmpty():
                msg = "Fileblock is empty: \n%s\n" % block
                msg += "Contains either no files or no SE Names\n"
                msg += "Will not be imported"
                logging.warning(msg)
                continue
            
            if self.splitType == "event":
                logging.debug(
                    "Inserting Fileblock split By Events: %s" % block
                    )
                jobDefs = splitter.splitByEvents(block, self.splitSize)
            else:
                logging.debug(
                    "Inserting Fileblock split By Files: %s" % block
                    )
                
                jobDefs = splitter.splitByFiles(block, self.splitSize)

            try:
                DatabaseAPI.insertJobs(owner, * jobDefs)
            except Exception, ex:
                msg = "Error inserting jobs into database for workflow:\n"
                msg += "%s\n" % self.workflowSpec.workflowName()
                msg += str(ex)
                logging.error(msg)
                return 1

        return 0
    
    def releaseJobs(self, numJobs):
        """
        _releaseJobs_

        Release the requested number of jobs.

        """
        owner = DatabaseAPI.ownerIndex(self.workflowSpec.workflowName())
        jobDefs = DatabaseAPI.retrieveJobDefs(owner, numJobs)
        return jobDefs
        
        
    def isComplete(self):
        """
        _isComplete_

        Does this dataset have any jobs left. If not, then it is complete

        """
        owner = DatabaseAPI.ownerIndex(self.workflowSpec.workflowName())
        if DatabaseAPI.countJobs(owner) > 0:
            return False
        return True

    def cleanup(self):
        """
        _cleanup_

        remove this workflow from the DB
        """
        DatabaseAPI.dropOwner(self.workflowSpec.workflowName())
        return


    def updateDataset(self):
        """
        _updateDataset_

        Look for new fileblocks not in the DB for this dataset and
        import them

        """
        owner = DatabaseAPI.ownerIndex(self.workflowSpec.workflowName())
        if owner == None:
            knownBlocks = []
        else:
            knownBlocks = DatabaseAPI.listKnownFileblocks(owner)

        logging.info("knownBlocks: %s" % str(knownBlocks))
        
        
        #  //
        # // Create a new splitter from the DBS/DLS containing all
        #//  current fileblocks and filter out the blocks that are
        #  //already known.
        # //
        #//
        try:
            splitter = createJobSplitter(self.inputDataset(),
                                         **self.dbsdlsContact)
        except Exception, ex:
            msg = "Unable to extract details from DBS/DLS for dataset:\n"
            msg += "%s\n" % self.inputDataset()
            msg += "Unable to update dataset: %s\n" % (
                self.workflowSpec.workflowName(),
                )
            msg += str(ex)
            logging.error(msg)
            return
        
        #  //
        # // filter out known blocks
        #//
        for fileblock in knownBlocks:
            if fileblock in splitter.fileblocks.keys():
                del splitter.fileblocks[fileblock]

        if len(splitter.fileblocks.keys()) == 0:
            #  //
            # // No new blocks
            #//
            msg = "There are no new blocks found for %s\n" % (
                self.workflowSpec.workflowName(),
                )
            msg += "Already up to date\n"
            logging.info(msg)
            return
        #  //
        # // Insert contents of splitter.
        #//
        logging.info("%s New fileblocks found for %s\n" % (
            len(splitter.fileblocks.keys()),
            self.workflowSpec.workflowName(),
            )
                     )
        logging.debug("New Blocks:\n %s\n" % str(splitter.fileblocks.keys()))
        self.insertSplitter(splitter, owner)
        return
    


    def save(self, directory):
        """
        _save_

        Save details of this object to the dir provided using
        the basename of the workflow file

        """
        workflowName =  self.workflowSpec.workflowName()
        fileBase = os.path.join(directory, workflowName)

        runFile = "%s.count" % fileBase
        
        handle = open(runFile, 'w')
        handle.write(str(self.count))
        handle.close()
        
        
        return


    def load(self, directory):
        """
        _load_

        For this instance, search for a params file in the dir provided
        using the workflow name in this instance, and if present, load its
        settings

        """
        workflowName =  self.workflowSpec.workflowName()
        fileBase = os.path.join(directory, workflowName)
        runFile = "%s.count" % fileBase

        runContent = readStringFromFile(runFile)
        logging.debug("Loaded Run number for workflow: %s" % runContent)
        if runContent != None:
            self.count = int(runContent)
        
        return
    

def readStringFromFile(filename):
    """
    _readStringFromFile_

    util to extract file content as a string

    """
    if not os.path.exists(filename):
        return None
    content = file(filename).read()
    content = content.strip()
    return content

        
        
                                         
        
