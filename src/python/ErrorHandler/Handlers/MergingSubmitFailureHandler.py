#!/usr/bin/env python

import logging

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.Registry import registerHandler
from JobState.Database.Api.RetryException import RetryException
from JobState.JobStateAPI import JobStateChangeAPI

class MergingSubmitFailureHandler(HandlerInterface):
    """
    _MergingSubmitFailureHandler_

    Merging submit error handler that either generates a new submit event
    or cleans out the job information if the maximum number of retries
    has been reached (and generates a general failure event).

    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobId = payload

         try:
              JobStateChangeAPI.submitFailure(jobId)
              logging.debug(">MergingSubmitFailureHandler<: Registered a "+\
                            " job submit failure,"\
                            "publishing a submit job event")
              self.publishEvent("SubmitJob",(jobId))
         except RetryException:
              logging.debug(">ProcessingRunFailureHandler<: Registered "+\
                            "a job submit failure "+ \
                            "Maximum number of retries reached!" +\
                            " Submitting a general failure job and cleanup event ")
              self.publishEvent("JobCleanup",(jobId))
              self.publishEvent("GeneralJobFailure",(jobId))

registerHandler(MergingSubmitFailureHandler(),"mergingSubmitFailureHandler")


