
#!/usr/bin/env python

import logging 

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.Registry import registerHandler
from ErrorHandler.Registry import retrieveHandler
from JobState.JobStateAPI import JobStateInfoAPI



class SubmitFailureHandler(HandlerInterface):
    """
    _SubmitFailureHandler_

    Handles job submit failures. Called by the error handler if a 
    job submit failure event is received.

    We distinguish two classes of failures. A failure that happens during
    a submission (job could not run), or a failure during running of the
    job.  the payload for SubmitFailure the job spec id which we use
    to retrieve its job type and associate an appropiate handler to it.

    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobId = payload

         logging.debug(">SubmitFailureHandler<: Retrieving jobId from payload: "+str(jobId))

         # we retrieve the job type, and depending on the type we
         # pick an error handler.

         jobType=JobStateInfoAPI.general(jobId)['JobType']
         handlerType=jobType+"SubmitFailureHandler"
         try:
              handler=retrieveHandler(handlerType)
              logging.debug(">SubmitFailureHandler<:Retrieved handler for jobId: "+ \
                            str(jobId)+" with job type: "+str(jobType))
         except:
              raise Exception("ERROR Submit","Could not find handler "+handlerType)

         handler.handleError(payload)

registerHandler(SubmitFailureHandler(),"submitFailureHandler")







