#!/usr/bin/env python
"""
_JobQueueBot_

Periodic tests and cleanup of the MessageService

"""

import logging

from AdminControl.Bots.BotInterface import BotInterface
from AdminControl.Registry import registerBot

from JobQueue.JobQueueDB import JobQueueDB
from ProdCommon.Database import Session


class JobQueueBot(BotInterface):
    """
    _JobQueueBot_

    Check MessageService health, clean out old messages from history.
    Generate Alerts for large message queues for a component, or
    outstanding messages etc.

    """
    def __init__(self):
        BotInterface.__init__(self)
        self.skipCycles = 1


    def __call__(self):
        """
        _operator()_

        Invoke Bot

        """
        logging.info(
            "JobQueueBot Invoked: removing release jobs > 24:00:00 old")
        jobQ = JobQueueDB()
        jobQ.cleanOut("24:00:00")
        Session.commit_all()
        

        return

    
registerBot(JobQueueBot, JobQueueBot.__name__)
