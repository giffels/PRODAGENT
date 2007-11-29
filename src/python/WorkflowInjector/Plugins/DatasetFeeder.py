#!/usr/bin/env python
"""
_DatasetFeeder_

Plugin to generate a fixed amount of production jobs from a workflow that
processes a dataset

The input to this plugin is a workflow that contains the following
parameters:

- SplitType     event or file
- SplitSize     number of events or files per job
- InputDataset  List of InputDataset
- DBSURL        URL of DBS Instance containing the datasets

"""

import logging

from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin

from ProdCommon.JobFactory.DatasetJobFactory import DatasetJobFactory



class DatasetFeeder(PluginInterface):
    """
    _DatasetFeeder_

    Generate a pile of processing style jobs based on the workflow
    and dataset provided

    """
    def handleInput(self, payload):
        logging.info("DatasetFeeder: Handling %s" % payload)
        self.workflow = None
        self.dbsUrl = None
        self.loadPayloads(payload)
        self.publishWorkflow(payload, self.workflow.workflowName())
        factory = DatasetJobFactory(self.workflow,
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
        
        
registerPlugin(DatasetFeeder, DatasetFeeder.__name__)



