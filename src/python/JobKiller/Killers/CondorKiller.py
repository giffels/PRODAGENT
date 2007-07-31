#!/usr/bin/env python
"""
_CondorKiller_

Killer plugin for killing condor jobs

"""


import logging
import os

from JobKiller.Registry import registerKiller
from JobKiller.KillerInterface import KillerInterface

import ProdAgent.WorkflowEntities.Job as WEJob
import ProdAgent.WorkflowEntities.Workflow as WEWorkflow



class CondorKiller:
    """
    _CondorKiller_

    """
    def condorRM(self, *jobIds):
        """
        _condorRM_

        Execute a condor_rm on the JobSpecId provided

        """
        command = ""
        for jobSpecId in jobIds:
            constraint = "\"ProdAgent_JobID == %s\"" % jobSpecId
            command  += "condor_rm -constraint %s;\n" % constraint
        os.system(command)
        return

    def deleteFromWE(self, *jobSpecIds):
        """
        _deleteFromWE_

        Delete the list of provided job spec Ids from WE tables

        """
        ids = list(jobSpecIds)
        WEJob.remove(ids)
        return
        

    def killJob(self, jobSpecId):
        logging.info("CondorKiller.killJob(%s)" % jobSpecId)
        self.condorRM(jobSpecId)
        return
        
    def killWorkflow(self, workflowSpecId):
        logging.info("CondorKiller.killWorkflow(%s)" % workflowSpecId)
        jobIds = WEWorkflow.getJobIDs(workflowSpecId)
        self.condorRM(*jobIds)
        return
    
        
    def eraseJob(self, jobSpecId):
        logging.info("CondorKiller.eraseJob(%s)" % jobSpecId)
        self.condorRM(jobSpecId)
        self.deleteFromWE(jobSpecId)

    def eraseWorkflow(self, workflowSpecId):
        logging.info("CondorKiller.eraseWorkflow(%s)" % workflowSpecId)
        jobIds = WEWorkflow.getJobIDs(workflowSpecId)
        self.condorRM(*jobIds)
        self.deleteFromWE(*jobIds)

    def killTask(self, taskSpecId):
        logging.info("CondorKiller.killTask(%s)" % taskSpecId)

registerKiller(CondorKiller, CondorKiller.__name__)
