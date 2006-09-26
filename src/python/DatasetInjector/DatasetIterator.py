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

        self.splitType = \
                self.workflowSpec.parameters.get("SplitType", "file").lower()
        self.splitSize = int(self.workflowSpec.parameters.get("SplitSize", 1))
        

        
        
        
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
            splitter = createJobSplitter(self.inputDataset())
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
        #  //
        # // Now insert data into DB
        #//
        logging.debug("SplitSize = %s" % self.splitSize)
        for block in splitter.listFileblocks():
            blockInstance = splitter.fileblocks[block]
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

    def save(self, directory):
        """
        _save_

        Save details of this object to the dir provided using
        the basename of the workflow file

        """
      
        return


    def load(self, directory):
        """
        _load_

        For this instance, search for a params file in the dir provided
        using the workflow name in this instance, and if present, load its
        settings

        """
        return
        
        
                                         
        
