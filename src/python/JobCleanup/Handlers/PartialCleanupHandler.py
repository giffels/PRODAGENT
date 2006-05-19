#!/usr/bin/env python

import logging 
import os

from JobCleanup.Handlers.HandlerInterface import HandlerInterface
from JobCleanup.Registry import registerHandler
from JobCleanup.Registry import retrieveHandler

from JobState.JobStateAPI import JobStateInfoAPI
from Trigger.TriggerAPI import TriggerAPI

class PartialCleanupHandler(HandlerInterface):
    """
    _PartialCleanupHandler_

    Does a partial cleanup of the job cache directory, to prevent
    it growing to large. The size is checked every time a submit or run
    failure occurs and resubmission needs to take place.
    """

    def __init__(self):
         HandlerInterface.__init__(self)
         self.args={}

    def handleEvent(self,payload):
         """
         The payload of a partial cleanup handler is a job id and
         the event (plus payload) it needs to emit aferwards. 
         """
         payloads=payload.split(',')
         jobId=payloads[0]
         nextEvent=payloads[1]
         nextPayload=payloads[2]

         try:
             logging.debug(">PartialCleanupHandler< removing cached files "+\
                            "for jobspec: "+str(jobId))
             cacheDirLocation=JobStateInfoAPI.general(str(jobId))['CacheDirLocation']
             logging.debug(">PartialCleanupHandler< starting remove in: "+cacheDirLocation)
             try:
                 for root, dirs, files in os.walk(cacheDirLocation, topdown=False):
                     for name in files:
                         # check if file is an .xml or .tar.gz file 
                         # if so do not remove.
                         # NOTE: should use reg. exp. here.
                         isSaved=False 
                         # we only keep files that are in the top dir.
                         # if we in the top dir we check for certain extensions.
                         if root==cacheDirLocation:
                             extensions=['.xml','.tar.gz']
                             for extension in extensions:
                                 pos1=name.rfind(extension)
                                 pos2=len(name)-len(extension)
                                 if(pos1==pos2):
                                     isSaved=True
                                     break

                         if not isSaved:
                             try:
                                 os.remove(os.path.join(root, name))
                             except Exception,ex:
                                 logging.debug(">PartialCleanupHandler< WARNING "+\
                                     " partial job cleanup: "+str(ex))
                         else:
                             logging.debug(">PartialCleanupHandler< not removing: "+name)
                     for name in dirs:
                         os.rmdir(os.path.join(root, name))
             except Exception,ex:
                 logging.debug(">PartialCleanupHandler< WARNING partial job cleanup: "+\
                     str(ex))
         except Exception,ex:
             logging.debug(">PartialCleanupHandler< ERROR partial job cleanup: "+\
                 str(ex))
         logging.debug(">PartialCleanupHandler< publishing event: "+nextEvent+\
             " with payload: "+nextPayload)
         self.publishEvent(nextEvent,nextPayload) 

registerHandler(PartialCleanupHandler(),"partialCleanupHandler")







