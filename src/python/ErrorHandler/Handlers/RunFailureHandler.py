#!/usr/bin/env python

import logging 
import os
import os.path
import shutil
import urllib

from ProdCommon.Core.GlobalRegistry import retrieveHandler
from ProdCommon.Core.GlobalRegistry import registerHandler
from ProdCommon.Core.ProdException import ProdException

from ProdAgent.WorkflowEntities import JobState
from FwkJobRep.ReportParser import readJobReport

from ErrorHandler.DirSize import dirSize
from ErrorHandler.DirSize import convertSize
from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.TimeConvert import convertSeconds

class RunFailureHandler(HandlerInterface):
    """
    _RunFailureHandler_

    Handles job run failures. Called by the error handler if a job run failure 
    event is received. We distinguish two classes of failures. A failure 
    that happens during a submission (job could not run), or a failure 
    during running of the job.  the payload for RunFailure is a url to 
    the job report.

    Based on the job report, we can retrieve the job id and use
    that to retrieve the job type in the database. 

    Processing error handler that either generates a new submit event
    or cleans out the job information if the maximum number of retries
    has been reached (and generates a general failure event).

    Using this information we propagate it to different job error handlers 
    associated to different job types, for further processing.

    """

    def __init__(self):
         HandlerInterface.__init__(self)
         self.args={}

    def handleError(self,payload):
         """
         The payload of a job failure is a url to the job report
         """
         jobReportUrl= payload
         # prepare to retrieve the job report file.
         # NOTE: we assume that the report file has a relative unique name
         # NOTE: if that is not the case we need to add a unique identifier to it.
         slash = jobReportUrl.rfind('/')
         fileName = jobReportUrl[slash+1:]
         urllib.urlretrieve(jobReportUrl, \
              self.args['jobReportLocation']+'/'+fileName)
         logging.debug(">RunFailureHandler<:Retrieving job report from %s " % jobReportUrl)

         jobReport=readJobReport(self.args['jobReportLocation']+'/'+fileName)
         #NOTE: is this the right way to extract the job id.
         jobId=jobReport[0].jobSpecId
         logging.debug(">RunFailureHandler<:Retrieving jobId from job report "+\
                       "(used to dynamically load error handler) " \
                       "jobId="+str(jobId))

         # create the jobReportLocation jobId hierarchy if not exists.
         pipe=os.popen("mkdir -p "+self.args['jobReportLocation']+'/'+jobId)
         pipe.close()
         # move the report file to this new location.
         pipe=os.popen("mv "+self.args['jobReportLocation']+'/'+fileName+" "+ \
                       self.args['jobReportLocation']+'/'+jobId)
         logging.debug(">RunFailureHandler<:Moving job report to permanent storage: " \
                       +self.args['jobReportLocation']+'/'+jobId)
         pipe.close()


         reportLocation=self.args['jobReportLocation']+'/'+ \
                           jobId+'/'+fileName
         generalInfo=JobState.general(jobId)
         # a submit event with delay
         delay=int(self.args['DelayFactor'])*(int(generalInfo['Retries']+1))
         delay=convertSeconds(delay) 
         logging.debug(">RunFailureHandler<: re-submitting with delay (h:m:s) "+\
                      str(delay))

         if self.args['ReportAction'] == 'move' :
            # count how many files are in the dir (to generate unique ids
            # when moving files
            try:
               lastID = len(os.listdir(os.path.dirname(payload)))
               target = os.path.join(os.path.dirname(payload),\
               os.path.basename(payload).split('.')[0] +\
               str(lastID) +\
               '.xml')
               logging.debug('Moving file: '+ payload + ' to: ' + target)
               shutil.move(payload,target)
            except:
               pass
         try:
              JobState.runFailure(jobId,jobReportLocation= reportLocation)

              # check the cache dir size. If it is beyond the threshold, purge it.
              dirSizeBytes=dirSize(generalInfo['CacheDirLocation'],0,0,0)
              dirSizeMegaBytes=convertSize(dirSizeBytes,'m')
              logging.debug(">RunFailureHandler<:Cache dir. size is "+\
                            str(dirSizeMegaBytes)+" MB. Maximum allowed is "+\
                            str(self.maxCacheDirSizeMB)+" MB ")
              jobspecfile="%s/%s-JobSpec.xml" % (generalInfo['CacheDirLocation'],jobId)
              # if necessary first a partial cleanup is done, which after it
              # is finished publishes the proper event.

              # retrieve the number of retries and publish
              if(float(dirSizeMegaBytes)>float(self.maxCacheDirSizeMB)):
                  newPayload=jobId+",SubmitJob,"+jobId+","+str(delay)
                  logging.debug(">RunFailureHandler<: Reached maximum cache size. "+\
                      "Performing partial cache cleanup first.")
                  self.publishEvent("PartialJobCleanup",newPayload,delay)
              else:
                  logging.debug(">RunFailureHandler<:Registered "+\
                                "a job run failure,"\
                                "publishing a submit job event")
                  self.publishEvent("SubmitJob",jobspecfile,delay)
#                 self.publishEvent("SubmitJob",(jobId),delay)
         except ProdException,ex:
              if(ex["ErrorNr"]==3013): 
                  logging.debug(">RunFailureHandler<:Registered "+\
                  "a job run failure "+ \
                  "Maximum number of retries reached!" +\
                  " Submitting a failure job and cleanup event ")
                  JobState.failed(jobId)
                  self.publishEvent("FailureCleanup",(jobId))
                  self.publishEvent("GeneralJobFailure",(jobId))


         # we retrieve the job type, and depending on the type we
         # pick an error handler (her we propagate the rest of the
         # the error handling.

         jobType=generalInfo['JobType']
         handlerType=jobType+"RunFailureHandler"
         try:
              handler=retrieveHandler(handlerType,"ErrorHandler")
         except:
              raise ProdException("Could not find handler "+handlerType)

         logging.debug(">RunFailureHandler<:Propagating error to new error handler for further "+\
                       " processing")
         handler.handleError(reportLocation)

registerHandler(RunFailureHandler(),"runFailureHandler","ErrorHandler")







