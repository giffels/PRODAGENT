#!/usr/bin/env python

import logging 
import os

from JobCleanup.Handlers.HandlerInterface import HandlerInterface
from JobCleanup.Registry import registerHandler
from JobCleanup.Registry import retrieveHandler

from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI
from Trigger.TriggerAPI import TriggerAPI

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

         try:
             logging.debug(">CleanupHandler< removing cached files and state "+\
                            "information for jobspec: "+str(payload))
             cacheDirLocation=JobStateInfoAPI.general(str(payload))['CacheDirLocation']
             logging.debug(">CleanupHandler< removing directory: "+cacheDirLocation)
             try:
                 for root, dirs, files in os.walk(cacheDirLocation, topdown=False):
                     for name in files:
                         os.remove(os.path.join(root, name))
                     for name in dirs:
                         os.rmdir(os.path.join(root, name))
                 os.rmdir(cacheDirLocation)
             except Exception,ex:
                 logging.debug(">CleanupHandler< WARNING job cleanup: "+str(ex))
             JobStateChangeAPI.cleanout(str(payload))
         except Exception,ex:
             logging.debug(">CleanupHandler< ERROR job cleanup: "+str(ex))

registerHandler(CleanupHandler(),"cleanupHandler")







