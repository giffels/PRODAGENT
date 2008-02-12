 #!/usr/bin/env python
"""
_JobEmulatorTracker_

Tracker for Job Emulator submissions.

"""

__revision__ = "$Id: "
__version__ = "$Revision: "

import logging

from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker

from JobEmulator.JobEmulatorAPI import queryJobsByID
from JobEmulator.JobEmulatorAPI import removeJob

class JobEmulatorTracker(TrackerPlugin):
    """
    _JobEmulatorTracker_

    Poll the Job Emulator for tracking information

    """
    def __init__(self):
        TrackerPlugin.__init__(self)
        self.classads = None
        self.cooloff = "00:1:00"

    def initialise(self):
        """
        _initialise_

        """
        pass
        
    def updateSubmitted(self, *submitted):
        """
        _updateSubmitted_

        Override to look at each submitted state job spec id provided
        and change its status if reqd.

        """
        logging.info("JobEmulator: Submitted Count: %s" % len(submitted))

        for subId in submitted:
            jobInfo = queryJobsByID(subId)

            if jobInfo == []:
                self.TrackerDB.removeJob(subId)
                continue

            if len(jobInfo) != 1:
                # We should only get one job back when we
                # query by ID.
                logging.error("Error: Found multiple jobs with ID %s" % subId)
                logging.error("%s" % jobInfo)
                continue
            
            jobState = jobInfo[0][3]

            if jobState == "new":
                self.TrackerDB.jobRunning(subId)
            elif jobState == "failed":
                logging.debug("ggkk: %s failed" % subId)
                self.TrackerDB.jobFailed(subId)
                removeJob(subId)
            elif jobState == "finished":
                logging.debug("ggkk: %s complete" % subId)
                self.TrackerDB.jobComplete(subId)
                removeJob(subId)                
            else:
                logging.error("Unknown job state: %s" % jobState)
    
        return

    def updateRunning(self, *running):
        """
        _updateRunning_

        Check on Running Job

        """
        logging.info("JobEmulator: Running Count: %s" % len(running))

        for runId in running:
            jobInfo = queryJobsByID(runId)

            if jobInfo == []:
                self.TrackerDB.removeJob(runId)
                continue

            if len(jobInfo) != 1:
                # We should only get one job back when we
                # query by ID.
                logging.error("Error: Found multiple jobs with ID %s" % runId)
                logging.error("%s" % jobInfo)
                continue

            jobState = jobInfo[0][3]

            if jobState == "new":
                continue
            elif jobState == "failed":
                logging.debug("ggkk: %s failed" % runId)
                self.TrackerDB.jobFailed(runId)
                removeJob(runId)                
            elif jobState == "finished":
                logging.debug("ggkk: %s complete" % runId)
                self.TrackerDB.jobComplete(runId)
                removeJob(runId)                
            else:
                logging.error("Unknown job state: %s" % jobState)

        return

    def updateComplete(self, *complete):
        """
        _updateComplete_

        Take any required action on completion.

        Note: Do not publish these to the PA as success/failure, that
        is handled by the component itself

        """
        if len(complete) == 0:
            return
        summary = "Jobs Completed:\n"
        for compId in complete:
            summary += " -> %s\n" % compId
        logging.info(summary)
        return

    def updateFailed(self, *failed):
        """
        _updateFailed_

        Take any required action for failed jobs on completion

        """
        pass

        return

    def kill(self, *toKill):
        """
        _kill_

        """
        pass

    def cleanup(self):
        """
        _cleanup_

        """
        pass
        
registerTracker(JobEmulatorTracker, JobEmulatorTracker.__name__)
