#!/usr/bin/env python
"""
_ARCKiller_

Killer plugin for killing ARC jobs

"""


import logging
import os

from JobKiller.Registry import registerKiller
from JobKiller.KillerInterface import KillerInterface

import ProdAgent.WorkflowEntities.Job as WEJob
import ProdAgent.WorkflowEntities.Workflow as WEWorkflow

from ProdAgent.Resources import ARC


class ARCKiller(KillerInterface):
    def ngkill(self, *jobIds):
        """
        Execute a ngkill on the JobSpecId provided

        """
        victims = ""
        for jobSpecId in jobIds:
            victims  += jobSpecId + " "
        if len(victims) > 0:
            ARC.executeCommand("ngkill " + victims)


    def deleteFromWE(self, *jobSpecIds):
        """
        Delete the list of provided job spec Ids from WE tables

        """
        ids = list(jobSpecIds)
        WEJob.remove(ids)
        return
        

    def killJob(self, jobSpecId):
        logging.info("ARCKiller.killJob(%s)" % jobSpecId)
        self.ngkill(jobSpecId)
        return
        
    def killWorkflow(self, workflowSpecId):
        logging.info("ARCKiller.killWorkflow(%s)" % workflowSpecId)
        jobIds = WEWorkflow.getJobIDs(workflowSpecId)
        self.ngkill(*jobIds)
        return
    
        
    def eraseJob(self, jobSpecId):
        logging.info("ARCKiller.eraseJob(%s)" % jobSpecId)
        self.ngkill(jobSpecId)
        self.deleteFromWE(jobSpecId)

    def eraseWorkflow(self, workflowSpecId):
        logging.info("ARCKiller.eraseWorkflow(%s)" % workflowSpecId)
        jobIds = WEWorkflow.getJobIDs(workflowSpecId)
        self.ngkill(*jobIds)
        self.deleteFromWE(*jobIds)

    def killTask(self, taskSpecId):
        logging.info("ARCKiller.killTask(%s)" % taskSpecId)

registerKiller(ARCKiller, ARCKiller.__name__)
