#!/usr/bin/env python

import logging 
import os
import tarfile

from JobCleanup.Handlers.HandlerInterface import HandlerInterface
from JobCleanup.Registry import registerHandler
from JobCleanup.Registry import retrieveHandler

from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI

class FailureCleanupHandler(HandlerInterface):
    """
    _FailureCleanupHandler_

    A handler that receiveds a job spec id, It uses this id to
    find the associated job cache, zip the files and move them to
    a failure archive for examination. 

    """

    def __init__(self):
         HandlerInterface.__init__(self)
         self.failureArchive = None
         self.args={}

    def handleEvent(self,payload):
         """
         The payload of for a cleanup handler is a job id. 
         """
         if self.failureArchive == None:
             logging.error("No Failure Archive set: Cannot Archive Job:\n %s" % payload)
             return
         try:
             logging.debug(">FailureCleanupHandler< archiving  "+\
                            "information for jobspec: "+str(payload))
             try:
                 os.makedirs(self.failureArchive)
             except:
                 pass
             cacheDirLocation=JobStateInfoAPI.general(str(payload))['CacheDirLocation']
             logging.debug(">FailureCleanupHandler< archiving and removing directory: "+cacheDirLocation)
             tar=tarfile.open(self.failureArchive+'/'+str(payload)+'.tar.gz','w:gz')
             tar.add(cacheDirLocation)
             try:
                 for root, dirs, files in os.walk(cacheDirLocation, topdown=False):
                     for name in files:
                         os.remove(os.path.join(root, name))
                     for name in dirs:
                         os.rmdir(os.path.join(root, name))
                 os.rmdir(cacheDirLocation)
             except Exception,ex:
                 logging.debug(">FailureCleanupHandler< WARNING job cleanup: "+str(ex))
             JobStateChangeAPI.cleanout(str(payload))
         except Exception,ex:
             logging.debug(">FailureCleanupHandler< ERROR job cleanup: "+str(ex))

registerHandler(FailureCleanupHandler(),"failureCleanupHandler")







