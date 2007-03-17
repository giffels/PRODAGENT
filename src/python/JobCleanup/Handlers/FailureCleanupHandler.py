#!/usr/bin/env python

import logging 
import os
import tarfile

from JobCleanup.Handlers.HandlerInterface import HandlerInterface
from ProdAgent.WorkflowEntities import JobState
from ProdCommon.Core.GlobalRegistry import registerHandler

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
             cacheDirLocation=JobState.general(str(payload))['CacheDirLocation']
             logging.debug(">FailureCleanupHandler< archiving and removing directory: "+cacheDirLocation)
             #NOTE: check what this does when it is repeated (e.g. after a crash)
             tar=tarfile.open(self.failureArchive+'/'+str(payload)+'.tar.gz','w:gz')
             short_root=cacheDirLocation.split('/')[-1]
             tar.add(cacheDirLocation,short_root)
             tar.close()
             try:
                 for root, dirs, files in os.walk(cacheDirLocation, topdown=False):
                     for name in files:
                         os.remove(os.path.join(root, name))
                     for name in dirs:
                         os.rmdir(os.path.join(root, name))
                 os.rmdir(cacheDirLocation)
             except Exception,ex:
                 logging.debug(">FailureCleanupHandler< WARNING job cleanup: "+str(ex))
             JobState.cleanout(str(payload))
             logging.debug(">FailureCleanupHandler< archived completed for jobspecID: "+str(payload))
         except Exception,ex:
             logging.debug(">FailureCleanupHandler< ERROR job cleanup: "+str(ex))

registerHandler(FailureCleanupHandler(),"failureCleanupHandler","JobCleanup")







