#!/usr/bin/env python
"""
_DatasetFeeder_

Plugin to generate a fixed amount of production jobs from a workflow that
processes a dataset

The input to this plugin is a workflow that contains the following
parameters:

- InputDataset  List of InputDataset
- DBSURL        URL of DBS Instance containing the datasets

Note that split type is one file per job at present


TODO: Provide plugin/hook system to allow for checks on file staging.
Initial thought is that this may be better done as a plugin for the JobQueue
and or ResourceMonitor
to wait for files to stage for a job and then release the job from the
queue.
Would need the job queue to have some way to list the input files needed
for each job.

"""

import logging

from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from ProdCommon.JobFactory.ReRecoJobFactory import ReRecoJobFactory



class ReRecoFeeder(PluginInterface):
    """
    _ReRecoFeeder_

    Generate a pile of processing style jobs based on the workflow
    and dataset provided

    """
    def handleInput(self, payload):
        logging.info("ReRecoFeeder: Handling %s" % payload)
        self.workflow = None
        self.dbsUrl = None
        self.loadPayloads(payload)
        self.publishWorkflow(payload, self.workflow.workflowName())
        factory = ReRecoJobFactory(self.workflow,
                                   self.workingDir,
                                   self.dbsUrl)
                                    
        jobs = factory()

        for job in jobs:
            self.queueJob(job['JobSpecId'], job['JobSpecFile'],
                          job['JobType'],
                          job['WorkflowSpecId'],
                          job['WorkflowPriority'],
                          *job['Sites'])
            
        
        return
        

    def loadPayloads(self, workflowFile):
        """
        _loadPayloads_
        
        
        """
        self.workflow = self.loadWorkflow(workflowFile)
        
        
        value = self.workflow.parameters.get("DBSURL", None)
        if value != None:
            self.dbsUrl = value

        if self.dbsUrl == None:
            msg = "Error: No DBSURL available for dataset:\n"
            msg += "Cant get local DBSURL and one not provided with workflow"
            logging.error(msg)
            raise RuntimeError, msg



        return
        
        
registerPlugin(ReRecoFeeder, ReRecoFeeder.__name__)



