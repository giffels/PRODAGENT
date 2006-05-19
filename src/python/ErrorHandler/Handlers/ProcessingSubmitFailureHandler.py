#!/usr/bin/env python

import logging

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.Registry import registerHandler

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

registerHandler(ProcessingSubmitFailureHandler(),"processingSubmitFailureHandler")







