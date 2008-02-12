#!/usr/bin/env python
"""
_BlackHoleNodePlugin_

Plugin for the Job Emulator to successfully complete
using random completion algorithm  of jobs that are submitted, 
always fail in a certain node.

"""

__revision__ = "$Id: $"
__version__ = "$Revision: $"

import datetime
from random import random

from JobEmulator.Registry import registerPlugin
from JobEmulator.JobCompletionPlugins.JobCompletionPluginInterface \
     import JobCompletionPluginInterface


class BlackHoleNodePlugin(JobCompletionPluginInterface):
    """
    _BlackHoleNodePlugin_

    Plugin for the Job Emulator to randomly complete jobs
    except jobs at some predefined node(s). All the jobs in that node
    will fail right away

    """

    def processJob(self, jobInfo=None, jobRunnigLocation=None):
        """
        _processJobs_

        Determine if a job's status should change, and return
        the new status.  This plugin will complete 90% of the
        jobs submitted, and fail the rest.

        The jobInfo parameter is a four element long list with
        the following items:
          jobInfo[0]: Job ID
          jobInfo[1]: Job type: processing, merge, cleanup
          jobInfo[2]: Start time
          jobInfo[3]: Job status: new, finished, failed                              
          
        """
        # process all the jobs immediately with success that 
        # have been in the job_emulator and always fail 'fakeHost17' 
         
        # need to add try block to check jobRunningLocation
        if jobRunnigLocation['HostName'].startswith('fakeHost_17.'):
            return "failed"
        
        # We start completing/failing jobs once a job has been in the
        # queue for 85% of its specified interval.
        completionHours, completionMinutes, completionSeconds = \
                       self.avgCompletionTime.split(":")
        completionInterval = int((int(completionHours) * 3600 + int(completionMinutes) \
                             * 60 + int(completionSeconds)) * 0.85)
        interval = datetime.timedelta(seconds=completionInterval)        
        if (datetime.datetime.now() - jobInfo[2]) < interval:
            return jobInfo[3]

        # We only complete/fail jobs 30% of the time.
        if random() < 0.7:
            return jobInfo[3]

        if random() < float(self.avgCompletionPercentage):
            return "finished"
        else:
            return "failed"


registerPlugin(BlackHoleNodePlugin, BlackHoleNodePlugin.__name__)