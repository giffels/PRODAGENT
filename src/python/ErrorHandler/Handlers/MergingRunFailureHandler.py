#!/usr/bin/env python

import logging 

from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.Core.GlobalRegistry import registerHandler


class MergingRunFailureHandler(HandlerInterface):
    """
    _MergingRunFailureHandler_

    Handles merge specific errors.
    """

    def __init__(self):
         HandlerInterface.__init__(self)

    def handleError(self,payload):
         jobReport=readJobReport(payload)

         if len(jobReport) == 0:
             logging.error("Error parsing FWJR: %s" % payload)
             
         jobId  = jobReport[0].jobSpecId
         logging.debug(">MergeRunFailureHandler<: do nothing 4 the moment")

registerHandler(MergingRunFailureHandler(),"MergeRunFailureHandler","ErrorHandler")







