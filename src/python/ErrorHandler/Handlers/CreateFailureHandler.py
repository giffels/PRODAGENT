
#!/usr/bin/env python

import logging

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.TimeConvert import convertSeconds
from ProdAgent.WorkflowEntities import JobState
from ProdCommon.Core.GlobalRegistry import registerHandler
from ProdCommon.Core.ProdException import ProdException


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
         generalInfo=JobState.general(jobId)

         delay=int(self.args['DelayFactor'])*(int(generalInfo['Retries']+1))
         delay=convertSeconds(delay)
         logging.debug(">CreateFailureHandler<: re-creating with delay "+\
             " (h:m:s) "+str(delay))
         try:
              JobState.createFailure(jobId)

              logging.debug(">CreateFailureHandler<: Registered "+\
                            "a create failure,"\
                            "publishing a create event")
              self.publishEvent("CreateJob",(jobId),delay)
         except ProdException,ex:
              if(ex["ErrorNr"]==3013):
                  logging.debug(">CreateFailureHandler<: Registered "+\
                  "a create failure "+ \
                  "Maximum number of retries reached!" +\
                  " Submitting a general failure and cleanup job event ")
                  self.publishEvent("FailureCleanup",(jobId))
                  self.publishEvent("GeneralJobFailure",(jobId))

registerHandler(CreateFailureHandler(),"createFailureHandler","ErrorHandler")







