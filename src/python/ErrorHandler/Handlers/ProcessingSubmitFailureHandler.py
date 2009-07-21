#!/usr/bin/env python

import logging

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ProdCommon.Core.GlobalRegistry import registerHandler

class ProcessingSubmitFailureHandler(HandlerInterface):
    """
    _ProcessingSubmitFailureHandler_

    Failure handler for dealing with specific submit failures of processing type 
    jobs.   
    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobId = payload
         logging.debug(">ProcessingSubmitFailureHandler<: do nothing 4 the moment") 

registerHandler(ProcessingSubmitFailureHandler(),"ProcessingSubmitFailureHandler","ErrorHandler")
registerHandler(ProcessingSubmitFailureHandler(),"RepackSubmitFailureHandler","ErrorHandler")
registerHandler(ProcessingSubmitFailureHandler(),"ExpressSubmitFailureHandler","ErrorHandler")
registerHandler(ProcessingSubmitFailureHandler(),"HarvestingSubmitFailureHandler","ErrorHandler")
registerHandler(ProcessingSubmitFailureHandler(),"SkimSubmitFailureHandler","ErrorHandler")
registerHandler(ProcessingSubmitFailureHandler(),"LogCollectSubmitFailureHandler","ErrorHandler")
registerHandler(ProcessingSubmitFailureHandler(),"CleanUpSubmitFailureHandler","ErrorHandler")






