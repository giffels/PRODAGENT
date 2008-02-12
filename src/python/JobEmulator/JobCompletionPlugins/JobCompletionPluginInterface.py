#!/usr/bin/env python
"""
_JobCompletionPluginInterface_

Interface for plugins to the Job Emulator
that processes jobs submitted to the Job
Emulator.

"""

__revision__ = "$Id: $"
__version__ = "$Revision: $"

class JobCompletionPluginInterface:
    """
    _JobCompletionPluginInterface_

    Interface for plugins to the Job Emulator
    that processes jobs submitted to the Job
    Emulator.
    """

    def __init__(self):
        self.avgCompletionTime = None
        self.avgCompletionPercentage = None

    def processJob(self, jobInfo, jobRunningLocation=None):
        """
        _processJob_

        Determine if a job's status should change, and return
        the new status.

        The jobInfo parameter is a four element long list with
        the following items:
          jobInfo[0]: Job ID
          jobInfo[1]: Job type: processing, merge, cleanup
          jobInfo[2]: Start time
          jobInfo[3]: Job status: new, finished, failed                              
        """
        
        fname = "JobEmulator.JobCompletionPluginInterface.processJobs"
        raise NotImplementedError, fname
