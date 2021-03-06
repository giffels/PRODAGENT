#!/usr/bin/env python
"""
_JobReportPluginInterface_

Interface to plugins for the Job Emulator
that generate job reports for completed
jobs.

"""
__revision__ = "$Id: JobReportPluginInterface.py,v 1.5 2008/06/06 15:00:10 sryu Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "sfoulkes, sryu"
import logging

import logging

class JobReportPluginInterface:
    """
    _JobReportPluginInterface_

    Interface to plugins for the Job Emulator
    that generate job reports for completed
    jobs.
    
    """
    def __init__(self):
        self.avgEventProcessingRate = None
        # thresholdForMerge is Tier0ReportPlugin specific
        # this shouldn't be in interface, but quick fix.
        self.thresholdForMerge = None
        
    def createSuccessReport(self, jobSpec, workerNodeInfo, reportFilePath):
        """
        _createSuccessReport_

        Create a job report that represents the successful
        completion of a job.

        """
        fname = "JobEmulator.FwkJobReportPluginInterface.createSuccessReport"
        raise NotImplementedError, fname

    def createFailureReport(self, jobSpec, workerNodeInfo, reportFilePath):
        """
        _createFailureReport_

        Create a job report that represents a failed job.

        """
        fname = "JobEmulator.FwkJobReportPluginInterface.createFailureReport"
        raise NotImplementedError, fname

    def setDefaultForNoneValue(self, parameterName, parameter, default=None):
        """
        _lookupParameter_

        Check to see if a parameter exists in the ProdAgent config.
        If it does, return its value, otherwise return the default
        value.
        Usage: setDefaultForNoneValue(parmeterName="LFN Name", 
                                      parameter=self.lfn, 
                                      default="/store/fake")
        """
        if parameter == None:
            logging.error("%s not set, reporter plugin is using default: %s" %
                          (parameterName, default))
            return default
        
        return parameter
