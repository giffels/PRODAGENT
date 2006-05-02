#!/usr/bin/env python

import logging 
import os

from JobCleanup.Handlers.HandlerInterface import HandlerInterface
from JobCleanup.Registry import registerHandler
from JobCleanup.Registry import retrieveHandler

class CleanupHandler(HandlerInterface):
    """
    _CleanupHandler_

    """

    def __init__(self):
         HandlerInterface.__init__(self)
         self.args={}

    def handleEvent(self,payload):
         """
         The payload of a job failure is a url to the job report
         """
         jobReportUrl= payload

         logging.debug(">CleanupHandler<")

registerHandler(CleanupHandler(),"cleanupHandler")







