
#!/usr/bin/env python

import logging

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.Registry import registerHandler
from JobState.Database.Api.RetryException import RetryException
from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI


class CreateFailureHandler(HandlerInterface):
    """
    _CreateFailureHandler_

    Error handler that gets called if the prod agent fails
    to create a job. This handler either generates a new create event
    or cleans out the job information if the maximum number of retries 
    has been reached (and generates a general failure event).

    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobId  = payload

         try:
              JobStateChangeAPI.createFailure(jobId)

              logging.debug(">CreateFailureHandler<: Registered "+\
                            "a create failure,"\
                            "publishing a create event")
              self.publishEvent("CreateJob",(jobId))
         except RetryException:
              logging.debug(">CreateFailureHandler<: Registered "+\
                            "a create failure "+ \
                            "Maximum number of retries reached!" +\
                            " Submitting a general failure and cleanup job event ")
              self.publishEvent("FailureCleanup",(jobId))
              self.publishEvent("GeneralJobFailure",(jobId))

registerHandler(CreateFailureHandler(),"createFailureHandler")







