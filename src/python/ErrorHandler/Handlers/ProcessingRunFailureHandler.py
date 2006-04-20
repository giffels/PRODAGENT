#!/usr/bin/env python

import logging

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.Registry import registerHandler
from FwkJobRep.ReportParser import readJobReport
from JobState.Database.Api.RetryException import RetryException
from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI


class ProcessingRunFailureHandler(HandlerInterface):
    """
    _ProcessingRunFailureHandler_

    Processing error handler that either generates a new submit event
    or cleans out the job information if the maximum number of retries
    has been reached (and generates a general failure event).

    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobReport=readJobReport(payload)
         jobId  = jobReport[0].jobSpecId

         try:
              JobStateChangeAPI.runFailure(jobId,jobReportLocation= payload)

              logging.debug(">ProcessingRunFailureHandler<: Registered "+\
                            "a job run failure,"\
                            "publishing a submit job event")
              self.publishEvent("SubmitJob",(jobId))
         except RetryException:
              JobStateChangeAPI.cleanout(jobId)
              logging.debug(">ProcessingRunFailureHandler<: Registered "+\
                            "a job run failure "+ \
                            "Maximum number of retries reached!" +\
                            " Submitting a failure job event to be handled"+\
                            " by the prodmanager")

              self.publishEvent("GeneralJobFailure",(jobId))

registerHandler(ProcessingRunFailureHandler(),"processingRunFailureHandler")







