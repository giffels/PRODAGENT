#!/usr/bin/env python

import logging 

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.Registry import registerHandler
from FwkJobRep.ReportParser import readJobReport

class MergingRunFailureHandler(HandlerInterface):
    """
    _MergingRunFailureHandler_

    Handles merge specific errors.
    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobReport=readJobReport(payload)
         jobId  = jobReport[0].jobSpecId

registerHandler(MergingRunFailureHandler(),"mergingRunFailureHandler")







