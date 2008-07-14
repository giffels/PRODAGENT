#!/usr/bin/env python

import logging 

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.Registry import registerHandler
from FwkJobRep.ReportParser import readJobReport
from JobState.Database.Api.RetryException import RetryException
from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI


class MergingRunFailureHandler(HandlerInterface):
    """
    _MergingRunFailureHandler_

    Merging error handler that either generates a new submit event
    or cleans out the job information if the maximum number of retries
    has been reached and generates a general failure event.

    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobReport=readJobReport(payload)
         jobId  = jobReport[0].jobSpecId

         try:
              JobStateChangeAPI.runFailure(jobId,jobReportLocation= payload)

              logging.debug(">MergingRunFailureHandler<: Registered a "+\
                            " job run failure,"\
                            "publishing a submit job event")

              self.publishEvent("SubmitJob",(jobId))
         except RetryException:
              logging.debug(">MergingRunFailureHandler<: Registered a "+\
                            " job run failure "+ \
                            "Maximum number of retries reached!" +\
                            " Submitting a general failure job and cleanup event ")
              self.publishEvent("JobCleanup",(jobId))
              self.publishEvent("GeneralJobFailure",(jobId))

registerHandler(MergingRunFailureHandler(),"mergingRunFailureHandler")







