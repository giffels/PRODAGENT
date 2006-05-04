#!/usr/bin/env python

import logging 
import os

from JobCleanup.Handlers.HandlerInterface import HandlerInterface
from JobCleanup.Registry import registerHandler
from JobCleanup.Registry import retrieveHandler

from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI

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

         try:
             logging.debug(">CleanupHandler< removing cached files and state "+\
                            "information for jobspec: "+str(payload))
             cacheDirLocation=JobStateInfoAPI.general(str(payload))['CacheDirLocation']
             logging.debug(">CleanupHandler< removing directory: "+cacheDirLocation)
             os.rmdir(cacheDirLocation)
             JobStateChangeAPI.cleanout(str(payload))
         except Exception,ex:
             logging.debug(">CleanupHandler< ERROR job cleanup: "+str(ex))

registerHandler(CleanupHandler(),"cleanupHandler")







