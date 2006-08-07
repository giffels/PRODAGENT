#!/usr/bin/env python
"""
_RequestIterator_

Maintain a Workflow specification, and when prompted,
generate a new concrete job from that workflow

The Workflow is stored as an MCPayloads.WorkflowSpec,
The jobs are JobSpec instances created from the WorkflowSpec

"""

import os

from MCPayloads.WorkflowSpec import WorkflowSpec
from MCPayloads.LFNAlgorithm import createUnmergedLFNs
from CMSConfigTools.CfgGenerator import CfgGenerator


class RequestIterator:
    """
    _RequestIterator_

    Working from a Generic Workflow template, generate
    concrete jobs from it, keeping in-memory history

    """
    def __init__(self, workflowSpecFile, workingDir):
        self.workflow = workflowSpecFile
        self.workingDir = workingDir
        self.count = 0
        self.currentJob = None
        self.sitePref = None

        #  //
        # // Initially hard coded, should be extracted from Component Config
        #//
        self.eventsPerJob = 10 
        
        self.workflowSpec = WorkflowSpec()
        self.workflowSpec.load(workflowSpecFile)
        

    def __call__(self):
        """
        _operator()_

        When called generate a new concrete job payload from the
        generic workflow and return it.

        """
        newJobSpec = self.createJobSpec()
        self.count += 1
        return newJobSpec


    def createJobSpec(self):
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
        if self.sitePref != None:
            jobSpec.addWhitelistSite(self.sitePref)
            
        
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
        jobCfg = generator(self.currentJob,
                           maxEvents = self.eventsPerJob,
                           firstRun = self.count)

        jobSpecNode.configuration = jobCfg.cmsConfig.asPythonString()
        jobSpecNode.loadConfiguration()
        
        return
    
