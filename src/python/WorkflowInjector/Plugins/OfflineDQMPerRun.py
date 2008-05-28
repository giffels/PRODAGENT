#!/usr/bin/env python
"""
_OfflineDQMPerRun_

Plugin for injecting a set of analysis jobs for Offline DQM tools.

These jobs are set to process the entire dataset in one job per run


"""


import logging




from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin
from ProdCommon.JobFactory.RunJobFactory import RunJobFactory


class OfflineDQMPerRun(PluginInterface):
    """
    _OfflineDQMPerRun_

    Plugin to generate a set of analysis jobs for a workflow
    containing an input dataset

    """
    def handleInput(self, payload):
        logging.info("OfflineDQMPerRun: Handling %s" % payload)

        self.workflow = None
        self.dbsUrl = None
        self.loadPayloads(payload)

        
        
        factory = RunJobFactory(self.workflow,
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

        siteName = self.workflow.parameters.get("OnlySites", None)
        if siteName == None:
            msg = "Error: No sitename provided for OfflineDQMByRun workflow"
            msg += "\n You need to add an OnlySites parameter containing\n"
            msg += "the site name where the jobs will run"
            logging.error(msg)
            raise RuntimeError, msg

        runtimeScript = "JobCreator.RuntimeTools.RuntimeOfflineDQMPerRun"
        self.workflow.payload.scriptControls['PostTask'].append(runtimeScript)
        
        return
        
registerPlugin(OfflineDQMPerRun, OfflineDQMPerRun.__name__)
