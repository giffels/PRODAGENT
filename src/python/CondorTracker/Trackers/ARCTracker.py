#!/usr/bin/env python
"""
_ARCTracker_

Tracker for Nordugrid ARC submissions


"""

import logging
import os

from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker
import CondorTracker.CondorTrackerDB as TrackerDB
from ProdAgent.WorkflowEntities import Job
import ProdCommon.FwkJobRep.ReportState as ReportState
from ProdAgent.Resources import ARC



class ARCTracker(TrackerPlugin):
    """
    _ARCTracker_

    Poll ARC for status of jobs

    """
    def __init__(self):
        TrackerPlugin.__init__(self)
        self.jobs = {}
        logging.debug("ARCTracker.__init__()")
        self.jobIdMap = ARC.jobIdMap()


    def initialise(self):
        self.jobs = self.getJobStatus()
        logging.debug("initialise: Retrieved status for %i Jobs" % len(self.jobs))


    def getJobStatus(self, jobSpecId = None):
        """
        Get a {jobSpecId: status} dictionary for jobSpecId, or all jobs if
        jobSpecId == None.

        """

        status = {}
        for job in ARC.getJobs():
            if (jobSpecId == None) or (job.jobSpecId == jobSpecId):
                status[job.jobSpecId] = job.status

        return status



    def getJobReport(self,localDir,jobSpecId):
        """
        Get the FrameworkJobReport.xml file for job 'jobSpecId' by ngcp and put
        it in firectory 'dir'.  Return the path to the local copy of the file,
        or 'None' if either the job wasn't found, or the ngcp command failed.

        """

        if jobSpecId not in self.jobIdMap.keys():
            logging.debug("getJobReport: Couldn't find job " + jobSpecId)
            return None

        # Get the FrameworkJobReport.xml file, copied to
        # arcId/FrameworkJobReport.xml by the wrapper script

        arcId = self.jobIdMap[jobSpecId]
        ngcp = "ngcp %s/FrameworkJobReport.xml %s/" % (arcId,localDir)
        logging.debug("getJobReport: " + ngcp)
        try:
            ARC.executeCommand(ngcp)
        except ARC.CommandExecutionError, s:
            msg = "getJobReport: Report File Not Found for %s: " + jobSpecId
            msg += "Command '%s' failed with exit status %s" % (ngcp, str(s))
            logging.warning(msg)
            return None

        logging.debug("getJobReport: Report file for %s copied to %s" % \
                      (jobSpecId, localDir))

        if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
            # Let's get a few additional files as well; they can be useful for
            # tracking down errors. 
            try:
                ARC.executeCommand("ngcp %s/run.log %s/" % (arcId,localDir))
                ARC.executeCommand("ngcp %s/output %s/" % (arcId,localDir))
                ARC.executeCommand("ngcp %s/errors %s/" % (arcId,localDir))
            except ARC.CommandExecutionError, s:
                msg = "getJobReport: Copying of additional files failed for job "
                msg += jobSpecId + ": "
                msg += "ngcp failed with exit status %s" % str(s)
                logging.warning(msg)

        return localDir + "/FrameworkJobReport.xml"


    def updateSubmitted(self, *submitted):
        """
        _updateSubmitted_

        Override to look at each submitted state job spec id provided
        and change its status if reqd.

        """
        logging.info("ARC: Submitted Count: %s" % len(submitted))
        for subId in submitted:
            status = self.jobs.get(subId, None)
  
            # Job not in getJobStatus() output, check job report
            if status == None:
                msg = "No Status entry for %s, checking job report" % subId
                logging.debug(msg)
                status = self.jobReportStatus(subId)
                
            # If status still None, declare job lost/failed
            if status == None:
                self.TrackerDB.jobFailed(subId)
                logging.debug("Job %s has been lost" % (subId))
                continue

            # Now examine the status value, not sure what these are, but act
            # accordingly
            if status == "PEND":
                # Still submitted, nothing to be done
                logging.debug("Job %s is pending" % (subId))
                continue 
            if status in ("RUN", "DONE"):
                # Is running or completed already, forward to running handler
                self.TrackerDB.jobRunning(subId)
                logging.debug("Job %s is running or finished" % (subId))
                continue
            if status == "EXIT":
                # Failed or Aborted
                logging.debug("Job %s is held..." % (subId))
                self.TrackerDB.jobFailed(subId)
                
        return


    def updateRunning(self, *running):
        """
        _updateRunning_

        Check on Running Job

        """
        logging.info("ARC: Running Count: %s" % len(running))
        for id in running:

            status = self.jobs.get(id, None)
  
            # Job not in getJobStatus output, check job report
            if status == None:
                msg = "No Status entry for %s, checking job report" % id
                logging.debug(msg)
                status = self.jobReportStatus(id)
                
            # If status still None, declare job lost/failed
            if status == None:
                self.TrackerDB.jobFailed(id)
                logging.debug("Job %s has been lost" % (id))
                continue
            
            if status == "RUN":
                # Is running
                logging.debug("Job %s is still running" % (id))
                continue
            if status == "DONE":
                # Is Complete 
                self.TrackerDB.jobComplete(id)
                report = self.findJobReport(id, True)
                logging.debug("Job %s complete with report %s" % (id,report))
                continue
            if status == "EXIT":
                logging.debug("Job %s is held..." % (id))
                self.TrackerDB.jobFailed(id)

            
    def updateComplete(self, *complete):
        """
        _updateComplete_

        Take any required action on completion.

        Note: Do not publish these to the PA as success/failure, that
        is handled by the component itself

        """
        logging.debug("ARCTracker.updateComplete %i" % len(complete))
        if len(complete) == 0:
            return

        summary = "Jobs Completed:\n"
        for id in complete:
            try:
                cmd = "ngclean " + id
                ARC.executeCommand(cmd)
            except ARC.CommandExecutionError, s:
                msg = "Cleaning up of job %s failed; " % id
                msg += "command '%s' failed with exit status %s" % (cmd, str(s))
                logging.warning(msg)
            summary += " -> %s\n" % id

            arcId = self.jobIdMap.get(id, None)
            ARC.clearNoInfo(arcId)

        logging.info(summary)
        return


    def updateFailed(self, *failed):
        """
        _updateFailed_

        Take any required action for failed jobs on completion

        """
        logging.debug("ARCTracker.updateFailed %i" % len(failed))
        if len(failed) == 0:
            return

        summary = "Jobs Failed:\n"
        for id in failed:
            summary += " -> %s\n" % id

            arcId = self.jobIdMap.get(id, None)
            ARC.clearNoInfo(arcId)

        logging.debug(summary)
        return

        
    def findJobReport(self, jobSpecId, allwaysCopy = False):
        """
        _findJobReport_
        
        Given a job spec Id, find the location of the job report file if it
        exists.  Return the path of the file.  If not found, return None

        """
        cache = self.getJobCache(jobSpecId)
        logging.debug("findJobReport, cache: " + str(cache))
        if cache == None:
            logging.debug("No JobCache found for Job Spec ID: %s" % jobSpecId)
            return None

        reportFile = "%s/FrameworkJobReport.xml" % cache
        if allwaysCopy or (not os.path.exists(reportFile)):
            reportFile = self.getJobReport(cache,jobSpecId)

        return reportFile

    
    def jobReportStatus(self, jobSpecId):
        """
        _jobReportStatus_

        Find the job report and determine the status of the job if
        possible.  Should return a StatusCode if a status is available, if
        the file cannot be found, return None

        """
        report = self.findJobReport(jobSpecId)
        logging.debug("jobReportStatus, report: " + str(report))
        if report == None:
            return None
        
        if ReportState.checkSuccess(report):
            return ARC.StatusCodes["FINISHED"]
        return ARC.StatusCodes["FAILED"]


registerTracker(ARCTracker, ARCTracker.__name__)
