#!/usr/bin/env python
"""
_BOSSKiller_

Killer plugin for killing BOSS jobs

"""

import logging

from JobKiller.Registry import registerKiller
from JobKiller.KillerInterface import KillerInterface



class BOSSKiller:
    """
    _BOSSKiller_

    """
    def killJob(self, jobSpecId):
        logging.info("BOSSKiller.killJob(%s)" % jobSpecId)

    def killWorkflow(self, workflowSpecId):
        logging.info("BOSSKiller.killWorkflow(%s)" % workflowSpecId)

    def eraseJob(self, jobSpecId):
        logging.info("BOSSKiller.eraseJob(%s)" % jobSpecId)

    def eraseWorkflow(self, workflowSpecId):
        logging.info("BOSSKiller.eraseWorkflow(%s)" % workflowSpecId)


registerKiller(BOSSKiller, BOSSKiller.__name__)
