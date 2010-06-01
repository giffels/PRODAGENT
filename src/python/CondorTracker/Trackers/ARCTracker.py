#!/usr/bin/env python
"""
_ARCTracker_

Tracker for Nordugrid ARC submissions


"""

import logging
import os
import time

from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker
import CondorTracker.CondorTrackerDB as TrackerDB
from ProdAgent.WorkflowEntities import Job
import ProdCommon.FwkJobRep.ReportState as ReportState
from ProdAgent.Resources import ARC
from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo



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
        toDashboard = {}
        for jid in submitted:
            status = self.jobs.get(jid, None)
  
            # Job not in getJobStatus() output, check job report
            if status == None:
                msg = "No Status entry for %s, checking job report" % jid
                logging.debug(msg)
                status = self.jobReportStatus(jid)
                
            # If status still None, declare job lost/failed
            if status == None:
                self.TrackerDB.jobFailed(jid)
                logging.debug("Job %s has been lost" % (jid))
                toDashboard['StatusValue'] = 'Aborted'
                self.publishStatusToDashboard(jid, toDashboard)
                continue

            # Now examine the status value, not sure what these are, but act
            # accordingly
            if status == "PEND":
                # Still submitted, nothing to be done
                logging.debug("Job %s is pending" % (jid))
                toDashboard['StatusValue'] = 'Submitted'
                self.publishStatusToDashboard(jid, toDashboard)
            elif status == "RUN":
                self.TrackerDB.jobRunning(jid)
                logging.debug("Job %s is running" % (jid))
                toDashboard['StatusValue'] = 'Running'
                self.publishStatusToDashboard(jid, toDashboard)
            elif status == "DONE":
                # Let's forward the handling of finished jobs to
                # 'updateRunning'.
                self.TrackerDB.jobRunning(jid)
                logging.debug("Job %s is finished" % (jid))
            elif status == "EXIT":
                # Failed or Aborted
                logging.debug("Job %s is held..." % (jid))
                self.TrackerDB.jobFailed(jid)
                toDashboard['StatusValue'] = 'Aborted'
                self.publishStatusToDashboard(jid, toDashboard)
            else:
                # I don't think this is supposed to be possible happen!
                logging.warning("WTF? Job %s has status %s" % (jid, status))
                
        return


    def updateRunning(self, *running):
        """
        _updateRunning_

        Check on Running Job

        """
        logging.info("ARC: Running Count: %s" % len(running))
        toDashboard = {}
        for jid in running:

            status = self.jobs.get(jid, None)
  
            # Job not in getJobStatus output, check job report
            if status == None:
                msg = "No Status entry for %s, checking job report" % jid
                logging.debug(msg)
                status = self.jobReportStatus(jid)
                
            # If status still None, declare job lost/failed
            if status == None:
                self.TrackerDB.jobFailed(jid)
                logging.debug("Job %s has been lost" % (jid))
                toDashboard['StatusValue'] = 'Aborted'
                self.publishStatusToDashboard(jid, toDashboard)
            elif status == "RUN":
                logging.debug("Job %s is still running" % (jid))
                toDashboard['StatusValue'] = 'Running'
                self.publishStatusToDashboard(jid, toDashboard)
            elif status == "DONE":
                self.TrackerDB.jobComplete(jid)
                report = self.findJobReport(jid, True)
                logging.debug("Job %s complete with report %s" % (jid,report))
                toDashboard['StatusValue'] = 'Done'
                self.publishStatusToDashboard(jid, toDashboard)
            elif status == "EXIT":
                logging.debug("Job %s is held..." % (jid))
                self.TrackerDB.jobFailed(jid)
                toDashboard['StatusValue'] = 'Aborted'
                self.publishStatusToDashboard(jid, toDashboard)
            else:
                logging.warning("WTF? Job %s has status %i" % (jid, status))

            
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


    def publishStatusToDashboard(self, jobSpecId, data):
        """
        _publishStatusToDashboard_

        Publish the dashboard info to the appropriate destination

        """
        #  //
        # // Check for dashboard usage
        #//
        self.usingDashboard = {'use' : 'True',
                               'address' : 'cms-pamon.cern.ch',
                               'port' : 8884}
        try:
            submitterConfig = loadPluginConfig("JobSubmitter", "Submitter")
            dashboardCfg = submitterConfig.get('Dashboard', {})
            self.usingDashboard['use'] = dashboardCfg.get(
                "UseDashboard", "False")
            self.usingDashboard['address'] = dashboardCfg.get(
                "DestinationHost")
            self.usingDashboard['port'] = int(dashboardCfg.get(
                "DestinationPort"))
            logging.debug("dashboardCfg = " + str(self.usingDashboard) )
        except:
            logging.info("No Dashboard section in SubmitterPluginConfig")
            logging.info("Taking default values:")
            logging.info("dashboardCfg = " + str(self.usingDashboard))

        if self.usingDashboard['use'].lower().strip() == "false":
            logging.info("Skipping Dasboard report.")
            return

        # Instantiate DashboardInfo strcuture
        dashboardInfo = DashboardInfo()

        # Get DashboardInfo file in jobCache dir
        jobCache = self.getJobCache(jobSpecId)
        dashboardInfoFile = os.path.join(jobCache, "DashboardInfo.xml")
        if not os.path.exists(dashboardInfoFile):
            msg = "Dashboard Info file not found\n"
            msg += "%s\n" % dashboardInfoFile
            msg += "Skipping dashboard report for %s\n" % jobSpecId
            logging.debug(msg)
            return
        try:
            dashboardInfo.read(dashboardInfoFile)
        except Exception, msg:
            logging.error(
                "Couldn't read dashboardInfoFile for job %s. %s" % (
                    jobSpecId, str(msg))
            )
            msg = "Skipping dashboard report for %s\n" % jobSpecId
            logging.debug(msg)
            return


        # Fill dashboard info
        oldStatus = dashboardInfo.get('StatusValue', '')
        dashboardInfo['StatusValue'] = data.get('StatusValue', '')

        statusReasonMap = {
            'Aborted': 'Job has been aboorted.',
            'Submitted': 'Job has been submitted.',
            'Done': 'Job terminated succesfully.',
            'Running': 'Job is running.'
        }
        if data.get('StatusValueReason', ''):
            dashboardInfo['StatusValueReason'] = \
                data.get('StatusValueReason', '')
        else:
            dashboardInfo['StatusValueReason'] = statusReasonMap.get(
                dashboardInfo['StatusValue'], '')

        if data.get('StatusEnterTime', ''):
            dashboardInfo['StatusEnterTime'] = data.get('StatusEnterTime', '')
        elif oldStatus != dashboardInfo['StatusValue']:
            dashboardInfo['StatusEnterTime'] = time.time()
        if data.get('StatusDestination', ''):
            if data['StatusDestination'].lower().find('unknown') == -1:
                dashboardInfo['StatusDestination'] = data['StatusDestination']

        # Broadcasting data
        try:
            dashboardInfo.publish(1)
            logging.debug("dashboard info sent for job %s" % jobSpecId)
        except Exception, msg:
            logging.error(
                "Cannot publish dashboard information for job %s. %s" % (
                    jobSpecId, str(msg))
            )

        # update DasboardInfo file
        logging.debug("Creating dashboardInfoFile %s." % dashboardInfoFile)
        try:
            dashboardInfo.write(dashboardInfoFile)
        except Exception, msg:
            logging.error(
                "Couldn't create dashboardInfoFile for job %s. %s" % (
                    jobSpecId, str(msg))
            )

        logging.debug("Information published in Dashboard:")
        msg = "\n - task: %s\n - job: %s" % (dashboardInfo.task,
            dashboardInfo.job)
        for key, value in dashboardInfo.items():
            msg += "\n - %s: %s" % (key, value)
        logging.debug(msg)

        return


registerTracker(ARCTracker, ARCTracker.__name__)
