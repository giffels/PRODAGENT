#!/usr/bin/env python

import logging

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.Registry import registerHandler

class MergingSubmitFailureHandler(HandlerInterface):
    """
    _MergingSubmitFailureHandler_

    Handler for dealing with specific submit failures of merge type jobs.

    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobId = payload
         logging.debug(">MergeSubmitFailureHandler<: do nothing 4 the moment")

registerHandler(MergingSubmitFailureHandler(),"MergeSubmitFailureHandler")


