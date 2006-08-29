#!/usr/bin/env python
"""
_DatasetIterator_

Maintain a Workflow specification, and when prompted,
generate a new concrete job from that workflow based on a JobDefinition object
defining the input LFNs and event range



"""

import os

from MCPayloads.WorkflowSpec import WorkflowSpec
from MCPayloads.LFNAlgorithm import createUnmergedLFNs
from CMSConfigTools.CfgGenerator import CfgGenerator


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
            'firstRun' : self.count,
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
    
