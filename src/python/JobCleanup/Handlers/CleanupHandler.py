#!/usr/bin/env python

import logging 
import os
import tarfile

from JobCleanup.Handlers.HandlerInterface import HandlerInterface
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job
from ProdCommon.Core.GlobalRegistry import registerHandler

class CleanupHandler(HandlerInterface):
    """
    _CleanupHandler_

    A handler that receiveds a job spec id, It uses this id to 
    call the cleanout methods of the database and find the cache dir
    which it removes.

    """

    def __init__(self):
         HandlerInterface.__init__(self)
         self.args={}

    def handleEvent(self,payload):
         """
         The payload of for a cleanup handler is a job id. 
         """
         if self.successArchive == None:
             logging.error("No Success Archive set: Cannot Archive Job:\n %s" % payload)
             return
         try:
             logging.debug(">CleanupHandler< archiving  "+\
                            "information for jobspec: "+str(payload))
             try:
                 os.makedirs(self.successArchive)
             except:
                 pass

             logging.debug(">CleanupHandler< removing cached files and state "+\
                            "information for jobspec: "+str(payload))
             cacheDirLocation=JobState.general(str(payload))['CacheDirLocation']
             logging.debug(">CleanupHandler< removing directory: "+cacheDirLocation)
             tar=tarfile.open(self.successArchive+'/'+str(payload)+'.tar.gz','w:gz')
             # there might not be a job tracking dir.
             try:
                 short_cacheDirLocation=cacheDirLocation.split('/')[-1]
                 tar.add(cacheDirLocation+'/JobTracking',short_cacheDirLocation+'/JobTracking')
             except:
                 pass
             try:
                 for root, dirs, files in os.walk(cacheDirLocation, topdown=False):
                     for name in files:
                         if root==cacheDirLocation:
                             short_root=cacheDirLocation.split('/')[-1]
                             #NOTE: should be done with regular expressions.
                             extensions=['.xml','.tar.gz']
                             if self.keepLogsInSuccessArchive:
                                 extensions+=['.err','.log','.out']
                             for extension in extensions:
                                 pos1=name.rfind(extension)
                                 pos2=len(name)-len(extension)
                                 if(pos1==pos2):
                                     tar.add(os.path.join(root,name),os.path.join(short_root,name))
                                     break
                         os.remove(os.path.join(root, name))
                     for name in dirs:
                         os.rmdir(os.path.join(root, name))
                 os.rmdir(cacheDirLocation)
                 tar.close()
             except Exception,ex:
                 logging.debug(">CleanupHandler< WARNING job cleanup: "+str(ex))
             logging.debug("trying to get bulkpath")
             job = Job.get(payload)
             logging.debug("trying to set flag for bulk clean (if possible)")
             self.trigger.setFlag("bulkClean",job['bulk_id'],payload, job['cache_dir'])
             Job.remove(str(payload))
         except Exception,ex:
             logging.debug(">CleanupHandler< ERROR job cleanup: "+str(ex))

registerHandler(CleanupHandler(),"cleanupHandler","JobCleanup")







