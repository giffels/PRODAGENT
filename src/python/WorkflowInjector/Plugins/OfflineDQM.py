#!/usr/bin/env python
"""
_OfflineDQM_

Plugin for injecting a set of analysis jobs for Offline DQM tools.

These jobs are set to process the entire dataset in a single job,
so the size of the dataset is pulled in and the split size
set to the number of files in the dataset


"""


import logging




from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin
from ProdCommon.JobFactory.DatasetJobFactory import DatasetJobFactory
from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery


class OfflineDQM(PluginInterface):
    """
    _OfflineDQM_

    Plugin to generate a set of analysis jobs for a workflow
    containing an input dataset

    """
    def handleInput(self, payload):
        logging.info("OfflineDQM: Handling %s" % payload)

        self.splitType = "file"
        self.splitSize = 10000000 # quick and nasty
        
        self.workflow = None
        self.dbsUrl = None
        self.loadPayloads(payload)

        
        
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
    
    def loadPayloads(self, payloadFile):
        """
        _loadPayloads_
        
        
        """
        
        
        
        self.workflow = self.loadWorkflow(payloadFile)
        
        
        value = self.workflow.parameters.get("DBSURL", None)
        if value != None:
            self.dbsUrl = value

        if self.dbsUrl == None:
            msg = "Error: No DBSURL available for dataset:\n"
            msg += "Cant get local DBSURL and one not provided with workflow"
            logging.error(msg)
            raise RuntimeError, msg

        self.workflow.parameters['SplitType'] = self.splitType
        self.workflow.parameters['SplitSize'] = self.splitSize

        runtimeScript = "JobCreator.RuntimeTools.RuntimeOfflineDQM"
        self.workflow.payload.scriptControls['PostTask'].append(runtimeScript)
        
        return
        
registerPlugin(OfflineDQM, OfflineDQM.__name__)
