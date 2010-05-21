#!/usr/bin/env python
"""
_T0LSFTracker_

Tracker for Tier-0 LSF  submissions


"""

import time
import logging
import fcntl, select, sys, os


from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker

import ProdCommon.FwkJobRep.ReportState as ReportState
from ProdCommon.FwkJobRep.ReportParser import readJobReport

from ProdAgent.Resources.LSF import LSFInterface
from ProdAgent.Resources.LSF import LSFStatus


class T0LSFTracker(TrackerPlugin):
    """
    _T0LSFTracker_

    Poll LSF bjobs command for status of T0 jobs

    """
    def __init__(self):
        TrackerPlugin.__init__(self)
        self.bjobs = {}
        self.cooloff = "00:05:00"


    def initialise(self):
        """
        _initialise_

        Retrieve data from bjobs command

        """
        self.bjobs = LSFInterface.bjobs()
        logging.debug("Retrieved %s Jobs" % len(self.bjobs))

    def updateSubmitted(self, *submitted):
        """
        _updateSubmitted_

        Override to look at each submitted state job spec id provided
        and change its status if reqd.

        """
        logging.info("T0LSF: Submitted Count: %s" % len(submitted))
        for subId in submitted:

            status = self.bjobs.get(subId, None)
  
            # job not in bjobs output, look at job report for backup
            if status == None:
                logging.debug("No LSF record for %s, checking job report" % (subId))
                status = self.jobReportStatus(subId)

            # if status still None, declare job lost/failed
            if status == None:
                logging.debug("Job %s has been lost" % (subId))
                self.TrackerDB.jobFailed(subId)

            # if submitted do nothing
            if status in (LSFStatus.submitted, LSFStatus.pend_suspend):
                logging.debug("Job %s is pending" % (subId))

            # if running or completed forward to running handler 
            elif status in (LSFStatus.running, LSFStatus.usr_suspend, LSFStatus.sys_suspend, LSFStatus.finished):
                logging.debug("Job %s is running" % (subId))
                self.TrackerDB.jobRunning(subId)

            # if failed mark as failed
            elif status == LSFStatus.failed:
                logging.debug("Job %s is held..." % (subId))
                self.TrackerDB.jobFailed(subId)

        return


    def updateRunning(self, *running):
        """
        _updateRunning_

        Check on Running Job

        """
        logging.info("T0LSF: Running Count: %s" % len(running))
        for runId in running:

            status = self.bjobs.get(runId, None)
  
            # job not in bjobs output, look at job report for backup
            if status == None:
                logging.debug("No LSF record for %s, checking job report" % (runId))
                status = self.jobReportStatus(runId)
                
            # if status still None, declare job lost/failed
            if status == None:
                logging.debug("Job %s has been lost" % (runId))
                self.TrackerDB.jobFailed(runId)

            # if running do nothing
            elif status in (LSFStatus.running, LSFStatus.usr_suspend, LSFStatus.sys_suspend):
                logging.debug("Job %s is still running" % (runId))

            # if finished check job report, then report status
            elif status == LSFStatus.finished:

                status = self.jobReportStatus(runId)

                if status == LSFStatus.finished:
                    logging.debug("Job %s finished." % (runId))
                    self.TrackerDB.jobComplete(runId)
                elif status == LSFStatus.failed:
                    logging.debug("Job %s finished ok, bad job report, marked as failed" % (runId))
                    self.TrackerDB.jobFailed(runId)
                elif status == None:
                    logging.debug("Job %s finished ok, no job report, marked as failed" % (runId))
                    self.TrackerDB.jobFailed(runId)

            # if failed mark as failed
            elif status == LSFStatus.failed:
                    logging.debug("Job %s failed" % (runId))
                    self.TrackerDB.jobFailed(runId)

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
        if len(failed) == 0:
            return

        summary = "Jobs Failed:\n"
        for compId in failed:
            summary += " -> %s\n" % compId
        logging.info(summary)

        return

    def kill(self, *toKill):
        """
        _kill_

        Lookup the cluster ID and do a bkill for each job spec ID provided

        """
        #TDB

    def cleanup(self):
        """
        _cleanup_

        """
        pass
        
        
    def findJobReport(self, jobSpecId):
        """
        _findJobReport_
        
        Given a job spec Id, find the location of the job report file if it exists.
        Return the path of the file.
        If not found, return None
        
        """
        cache = self.getJobCache(jobSpecId)
        if cache == None:
            logging.debug("No JobCache found for Job Spec ID: %s" % jobSpecId)
            return None
        reportFile = "%s/FrameworkJobReport.xml" % cache
        if not os.path.exists(reportFile):
            logging.debug("Report File Not Found: %s" % reportFile)
            return None
        return reportFile
    
    def jobReportStatus(self, jobSpecId):
        """
        _jobReportStatus_

        Find the job report and determine the status of the job if possible.
        Should return an LSF Status if a status is available, 
            1- if the file cannot be found, return None
            2- check if the job was successful --> return finished. If it 
               wasn't successful, determine whether if it we have bad FJR
               or not. 
            2a- check if the FJR can be opened, return failed if not.
            2b- if report is empty, return failed
            2c- if report is not empty and could be opened, return finished.

        """
        report = self.findJobReport(jobSpecId)
        if report == None:
            return None

        #
        # Don't trust JobSuccess for now
        #
        # If report has 'JobSuccess' then it's finished 
##         if ReportState.checkSuccess(report):
##             return LSFStatus.finished

        # if checkSuccess returns False, check if it's a bad report file
        try:
            reports = readJobReport(report)
            # if reports is empty --> failed
            if len(reports) == 0:
                return LSFStatus.failed

            for report in reports:
                # Only DQMHarvesting Jobs should fall into this category!
                # LogCollector and CleanUp jobs also have no output files.
                # PromptCalib jobs also have no edm output, but should have
                # report.analysisFiles != 0 
                if len(report.files) == 0 and \
                    report.jobType not in ("CleanUp", "LogCollect", "Harvesting") and \
                    report.wasSuccess():
                    if not (jobSpecId.startswith("DQMHarvest-") or \
                           jobSpecId.startswith("PromptCalib")):
                        logging.debug("Non-DQM, Non-PromptCalib job with no output files, mark as failed")
                        return LSFStatus.failed

            # if we are here, the report is good (JobFailed --> finished)
            return LSFStatus.finished
        except:
            # exception indicates bad report file => Implies failure
            return LSFStatus.failed

    

registerTracker(T0LSFTracker, T0LSFTracker.__name__)







