#!/usr/bin/env python
"""
_JobReportPluginInterface_

Interface to plugins for the Job Emulator
that generate job reports for completed
jobs.

"""
__revision__ = "$Id: $"
__version__ = "$Revision: $"


class JobReportPluginInterface:
    """
    _JobReportPluginInterface_

    Interface to plugins for the Job Emulator
    that generate job reports for completed
    jobs.
    
    """
    
    def createSuccessReport(self, jobSpec, jobRunningLocation):
        """
        _createSuccessReport_

        Create a job report that represents the successful
        completion of a job.

        """
        fname = "JobEmulator.FwkJobReportPluginInterface.createSuccessReport"
        raise NotImplementedError, fname

    def createFailureReport(self, jobSpec, jobRunningLocation):
        """
        _createFailureReport_

        Create a job report that represents a failed job.

        """
        fname = "JobEmulator.FwkJobReportPluginInterface.createFailureReport"
        raise NotImplementedError, fname
