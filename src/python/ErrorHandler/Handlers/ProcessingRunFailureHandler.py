#!/usr/bin/env python

import logging

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.Core.GlobalRegistry import registerHandler

class ProcessingRunFailureHandler(HandlerInterface):
    """
    _ProcessingRunFailureHandler_

    Handles processing specific errors.
    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobReport=readJobReport(payload)

         if len(jobReport) == 0:
             logging.error("Error parsing FWJR: %s" % payload)
             
         jobId  = jobReport[0].jobSpecId
         # do nothing for the moment.
         logging.debug(">ProcessingRunFailureHandler< do nothing 4 the moment")

registerHandler(ProcessingRunFailureHandler(),"ProcessingRunFailureHandler","ErrorHandler")
registerHandler(ProcessingRunFailureHandler(),"LogCollectRunFailureHandler","ErrorHandler")
registerHandler(ProcessingRunFailureHandler(),"CleanUpRunFailureHandler","ErrorHandler")






