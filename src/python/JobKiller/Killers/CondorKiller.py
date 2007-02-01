#!/usr/bin/env python
"""
_CondorKiller_

Killer plugin for killing condor jobs

"""


import logging

from JobKiller.Registry import registerKiller
from JobKiller.KillerInterface import KillerInterface



class CondorKiller:
    """
    _CondorKiller_

    """
    def killJob(self, jobSpecId):
        logging.info("CondorKiller.killJob(%s)" % jobSpecId)

    def killWorkflow(self, workflowSpecId):
        logging.info("CondorKiller.killWorkflow(%s)" % workflowSpecId)

    def eraseJob(self, jobSpecId):
        logging.info("CondorKiller.eraseJob(%s)" % jobSpecId)

    def eraseWorkflow(self, workflowSpecId):
        logging.info("CondorKiller.eraseWorkflow(%s)" % workflowSpecId)


registerKiller(CondorKiller, CondorKiller.__name__)
