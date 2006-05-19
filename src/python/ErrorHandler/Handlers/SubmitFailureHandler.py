
#!/usr/bin/env python

import logging 

from ErrorHandler.DirSize import dirSize
from ErrorHandler.DirSize import convertSize
from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.Registry import registerHandler
from ErrorHandler.Registry import retrieveHandler
from JobState.Database.Api.RetryException import RetryException
from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI
from ProdAgentCore.ProdAgentException import ProdAgentException

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

         generalInfo=JobStateInfoAPI.general(jobId)
         try:
              JobStateChangeAPI.submitFailure(jobId)

              # check the cache dir size. If it is beyond the threshold, purge it.
              dirSizeBytes=dirSize(generalInfo['CacheDirLocation'],0,0,0)
              dirSizeMegaBytes=convertSize(dirSizeBytes,'m')
              logging.debug(">SubmitFailureHandler<:Cache dir. size is "+\
                            str(dirSizeMegaBytes)+" MB. Maximum allowed is "+\
                            str(self.maxCacheDirSizeMB)+" MB ")

              # if necessary first a partial cleanup is done, which after it
              # is finished publishes the proper event.
              if(float(dirSizeMegaBytes)>float(self.maxCacheDirSizeMB)):
                  newPayload=jobId+",SubmitJob,"+jobId
                  logging.debug(">SubmitFailureHandler<: Reached maximum cache size. "+\
                      "Performing partial cache cleanup first.")
                  self.publishEvent("PartialJobCleanup",newPayload)
              else:
                  logging.debug(">SubmitFailureHandler<:Registered "+\
                     "a job submit failure,"\
                     "publishing a submit job event")
                  self.publishEvent("SubmitJob",(jobId))
         except RetryException:
              logging.debug(">SubmitFailureHandler<:Registered "+\
                            "a job submit failure. "+ \
                            "Maximum number of retries reached!" +\
                            " Submitting a failure job and cleanup event ")
              self.publishEvent("JobCleanup",(jobId))
              self.publishEvent("GeneralJobFailure",(jobId))


         # we retrieve the job type, and depending on the type we
         # pick an error handler.

         jobType=generalInfo['JobType']
         handlerType=jobType+"SubmitFailureHandler"
         try:
              handler=retrieveHandler(handlerType)
         except:
              raise ProdAgentException("Could not find handler "+handlerType)
         logging.debug(">SubmitFailureHandler<:Propagating error to new  "+\
                       "error handler for further processing")
         handler.handleError(payload)

registerHandler(SubmitFailureHandler(),"submitFailureHandler")







