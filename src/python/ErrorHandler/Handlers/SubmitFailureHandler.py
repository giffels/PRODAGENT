
#!/usr/bin/env python

import logging 

from ErrorHandler.DirSize import dirSize
from ErrorHandler.DirSize import convertSize
from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.TimeConvert import convertSeconds
from JobState.Database.Api.RetryException import RetryException
from ProdAgent.WorkflowEntities import JobState
from ProdCommon.Core.GlobalRegistry import retrieveHandler
from ProdCommon.Core.GlobalRegistry import registerHandler
from ProdCommon.Core.ProdException import ProdException

from JobQueue import JobQueueAPI

class SubmitFailureHandler(HandlerInterface):
    """
    _SubmitFailureHandler_

    Handles job submit failures. Called by the error handler if a 
    job submit failure event is received.

    Processing submission error handler that either generates a new submit event
    or cleans out the job information if the maximum number of retries
    has been reached (and generates a general failure event).

    We distinguish two classes of failures. A failure that happens during
    a submission (job could not run), or a failure during running of the
    job.  the payload for SubmitFailure the job spec id which we use
    to retrieve its job type and associate an appropiate handler to it.

    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobId = payload

         logging.debug(">SubmitFailureHandler<:Retrieving jobId from payload: "+str(jobId))

         generalInfo=JobState.general(jobId)
         # a submit event with delay
         delay=int(self.args['DelayFactor'])*(int(generalInfo['Retries']+1))
         delay=convertSeconds(delay)
         logging.debug(">SubmitHandler<: Submitting with delay (h:m:s) "+\
                      str(delay))
         try:
              JobState.submitFailure(jobId)

              # check the cache dir size. If it is beyond the threshold, purge it.
              dirSizeBytes=dirSize(generalInfo['CacheDirLocation'],0,0,0)
              dirSizeMegaBytes=convertSize(dirSizeBytes,'m')
              logging.debug(">SubmitFailureHandler<:Cache dir. size is "+\
                            str(dirSizeMegaBytes)+" MB. Maximum allowed is "+\
                            str(self.maxCacheDirSizeMB)+" MB ")
              jobspecfile="%s/%s-JobSpec.xml" % (generalInfo['CacheDirLocation'],jobId)
              # if necessary first a partial cleanup is done, which after it
              # is finished publishes the proper event.
              if(float(dirSizeMegaBytes)>float(self.maxCacheDirSizeMB)):
                  newPayload=jobId+",SubmitJob,"+jobId
                  logging.debug(">SubmitFailureHandler<: Reached maximum cache size. "+\
                      "Performing partial cache cleanup first.")
                  self.publishEvent("PartialJobCleanup",newPayload,delay)
              else:
                  logging.debug(">SubmitFailureHandler<:Registered "+\
                     "a job submit failure,"\
                     "publishing a submit job event")
                  if self.args['QueueFailures']:
                      JobQueueAPI.reQueueJob(jobId)
                  else:
                      self.publishEvent("SubmitJob",jobspecfile,delay)

         except ProdException,ex:
              if(ex["ErrorNr"]==3013):
                  logging.debug(">SubmitFailureHandler<:Registered "+\
                  "a job submit failure. "+ \
                  "Maximum number of retries reached!" +\
                  " Submitting a failure job and cleanup event ")
                  JobState.failed(jobId)
                  self.publishEvent("FailureCleanup",(jobId))
                  self.publishEvent("GeneralJobFailure",(jobId))

         # we retrieve the job type, and depending on the type we
         # pick an error handler.

         jobType=generalInfo['JobType']
         handlerType=jobType+"SubmitFailureHandler"
         try:
              handler=retrieveHandler(handlerType,"ErrorHandler")
         except:
              raise ProdException("Could not find handler "+handlerType)
         logging.debug(">SubmitFailureHandler<:Propagating error to new  "+\
                       "error handler for further processing")
         handler.handleError(payload)

registerHandler(SubmitFailureHandler(),"submitFailureHandler","ErrorHandler")







