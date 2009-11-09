 #!/usr/bin/env python
"""
_CondorG_

Tracker for CondorG submissions


"""

import logging
import popen2
import os
import time

from ProdAgentCore.PluginConfiguration import loadPluginConfig

from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker
from CondorTracker.Trackers.CondorLog import readCondorLog

from ResourceMonitor.Monitors.CondorQ import condorQ

from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo




    

class CondorG(TrackerPlugin):
    """
    _CondorG_

    Poll condor G for tracking information

    """
    def __init__(self):
        TrackerPlugin.__init__(self)
        self.classads = None
        self.cooloff = "00:2:00"

    def initialise(self):
        """
        _initialise_

        Retrieve data from condor Q

        """
        
        constraint = "\"ProdAgent_JobID =!= UNDEFINED\""

        self.classads = condorQ(constraint)
        logging.info("Retrieved %s Classads" % len(self.classads))

    def updateSubmitted(self, *submitted):
        """
        _updateSubmitted_

        Override to look at each submitted state job spec id provided
        and change its status if reqd.

        """
        logging.info("CondorG: Submitted Count: %s" % len(submitted))
        for subId in submitted:
            status = None
            classad = self.findClassAd(subId)
            toDashboard = {}
            if classad == None:
                msg = "No Classad for %s, checking condor log" % subId
                logging.debug(msg)
                cache = self.getJobCache(subId)
                if cache == None:
                    msg = "Unable to find cache dir for job %s\n" % subId
                    msg += "Declaring job aborted"
                    logging.warning(msg)
                    toDashboard['StatusValue'] = 'Aborted'
                    self.publishStatusToDashboard(subId, toDashboard)
                    self.TrackerDB.jobFailed(subId)
                    continue
                # first check if shortened version exists...
                condorLogFile = "%s/condor.log" % cache
                if not os.path.exists(condorLogFile):
                   condorLogFile = "%s/%s-condor.log" % (cache, subId)
                if not os.path.exists(condorLogFile):
                    msg = "Cannot find condor log file:\n%s\n" % condorLogFile
                    msg += "Declaring job aborted"
                    logging.warning(msg)
                    toDashboard['StatusValue'] = 'Aborted'
                    self.publishStatusToDashboard(subId, toDashboard)
                    self.TrackerDB.jobFailed(subId)
                    continue
                
                condorLog = readCondorLog(condorLogFile)
                if condorLog == None:
                    msg = "Cannot read condor log file:\n%s\n" % condorLogFile
                    msg += "Not an XML log??\n"
                    msg += "Declaring job aborted"
                    logging.warning(msg)
                    toDashboard['StatusValue'] = 'Aborted'
                    self.publishStatusToDashboard(subId, toDashboard)
                    self.TrackerDB.jobFailed(subId)
                    continue
                #  //
                # // We have got the log file and computed an exit status 
                #// for it.
                #\\
                status = condorLog.condorStatus()
                clusterId = condorLog['Cluster']
                toDashboard['StatusValueReason'] = condorLog('Reason', '')

            else:
                status = classad['JobStatus']
                clusterId = classad['ClusterId']
                # Flling up Dashboard info
                toDashboard['StatusEnterTime'] = \
                    classad.get('EnteredCurrentStatus', '')
                toDashboard['StatusDestination'] = \
                    classad.get('MATCH_GLIDEIN_Gatekeeper', 'Unknown')
                toDashboard['RBname'] = \
                    classad.get('MATCH_GLIDEIN_Schedd', 'Unknown')
                

            if status == 1:
                toDashboard['StatusValue'] = 'Submitted'
                self.publishStatusToDashboard(subId, toDashboard)
                logging.debug("Job %s is pending" % (subId))
                continue 
            if status == 2:
                #  //
                # // Is running
                #//
                toDashboard['StatusValue'] = 'Running'
                self.publishStatusToDashboard(subId, toDashboard)
                self.TrackerDB.jobRunning(subId)
                logging.debug("Job %s is running" % (subId))
                continue
            if status == 4:
                #  //
                # // Is Complete -- but we want to forward to UpdateRunning first
                #// Dashboard report will also be done by UpdateRunning
                #\\
                self.TrackerDB.jobRunning(subId)
                logging.debug("Job %s complete" % (subId))
                continue
            if status == 5:
                #  //
                # // Held 
                #//
                logging.debug("Job %s is held..." % (subId))
#                self.TrackerDB.killJob(subId)
                command="condor_rm %s " % clusterId
                logging.debug("Removing job from queue...")
                logging.debug("Executing %s " % command)
                os.system(command)
                toDashboard['StatusValue'] = 'Aborted'
                self.publishStatusToDashboard(subId, toDashboard)
                self.TrackerDB.jobFailed(subId)
                continue
            if status in (3, 6):
                #  //
                # // Error or Removed
                #//
                toDashboard['StatusValue'] = 'Aborted'
                self.publishStatusToDashboard(subId, toDashboard)
                self.TrackerDB.jobFailed(subId)
                logging.debug("Job %s has failed" % (subId))
                continue

                
            if not status in range(1,6):
               logging.debug("Bad condor status flag: %s" % status)
               continue

            logging.debug("at end, status was %s" % status)
        return


    def updateRunning(self, *running):
        """
        _updateRunning_

        Check on Running Job

        """
        logging.info("CondorG: Running Count: %s" % len(running))
        for runId in running:
            toDashboard = {}
            classad = self.findClassAd(runId)
            if classad == None:
                msg = "No Classad for %s, checking condor log" % runId
                logging.debug(msg)
                cache = self.getJobCache(runId)
                if cache == None:
                    msg = "Unable to find cache dir for job %s\n" % runId
                    msg += "Declaring job aborted"
                    logging.warning(msg)
                    toDashboard['StatusValue'] = 'Aborted'
                    self.publishStatusToDashboard(runId, toDashboard)
                    self.TrackerDB.jobFailed(runId)
                    continue
                # first check if shortened version exists...
                condorLogFile = "%s/condor.log" % cache
                if not os.path.exists(condorLogFile):
                   condorLogFile = "%s/%s-condor.log" % (cache, runId)
                if not os.path.exists(condorLogFile):
                    msg = "Cannot find condor log file:\n%s\n" % condorLogFile
                    msg += "Declaring job aborted"
                    toDashboard['StatusValue'] = 'Aborted'
                    self.publishStatusToDashboard(runId, toDashboard)
                    logging.warning(msg)
                    self.TrackerDB.jobFailed(runId)
                    continue
                
                condorLog = readCondorLog(condorLogFile)
                if condorLog == None:
                    msg = "Cannot read condor log file:\n%s\n" % condorLogFile
                    msg += "Not an XML log??\n"
                    msg += "Declaring job aborted"
                    logging.warning(msg)
                    toDashboard['StatusValue'] = 'Aborted'
                    self.publishStatusToDashboard(runId, toDashboard)
                    self.TrackerDB.jobFailed(runId)
                    continue
                #  //
                # // We have got the log file and computed an exit status 
                #//  for it.
                #\\
                classad = {}
                classad['JobStatus'] = condorLog.condorStatus()
                classad['ClusterId'] = condorLog['Cluster']
                toDashboard['StatusValueReason'] = condorLog('Reason', '')
                
            status = classad['JobStatus']
            clusterId = classad['ClusterId']
            # Flling up Dashboard info
            toDashboard['StatusEnterTime'] = \
                classad.get('EnteredCurrentStatus', '')
            toDashboard['StatusDestination'] = \
                classad.get('MATCH_GLIDEIN_Gatekeeper', '')
            toDashboard['RBname'] = \
                classad.get('MATCH_GLIDEIN_Schedd', '')

            if status in (3, 6):
                #  //
                # // Removed or Error
                #//
                toDashboard['StatusValue'] = 'Aborted'
                self.publishStatusToDashboard(runId, toDashboard)
                self.TrackerDB.jobFailed(runId)
                if status == 3:
                     logging.debug("Job %s was removed, ClusterId=%s " % (runId,clusterId))
                else:
                     logging.debug("Job %s had an error, ClusterId=%s " % (runId,clusterId))
#                continue
            if status == 5:
                #  //
                # // Held
                #//
                logging.debug("Job %s is held..." % (runId))
                command="condor_rm %s " % clusterId
                logging.debug("Removing job from queue...")
                logging.debug("Executing %s " % command)
                os.system(command)
                toDashboard['StatusValue'] = 'Aborted'
                self.publishStatusToDashboard(runId, toDashboard)
                self.TrackerDB.jobFailed(runId)
            if status == 2:
                toDashboard['StatusValue'] = 'Running'
                self.publishStatusToDashboard(runId, toDashboard)
                logging.debug("Job %s is running, ClusterId=%s " % (runId,clusterId))
            if status == 4:
                toDashboard['StatusValue'] = 'Done'
                self.publishStatusToDashboard(runId, toDashboard)
                logging.debug("Job %s is complete, ClusterId=%s" % (runId,clusterId))
                self.TrackerDB.jobComplete(runId)
            if status == 1:
                oDashboard['StatusValue'] = 'Submitted'
                self.publishStatusToDashboard(runId, toDashboard)
                logging.debug("Job %s is idle? ClusterId=%s" % (runId,clusterId))
            if status > 6:
                logging.debug("Job %s status was %i, ClusterId=%s" % (runId,status,clusterId))
            
            
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

        Lookup the cluster ID and do a condor_rm for each job spec ID provided

        """
        #TDB

    def cleanup(self):
        """
        _cleanup_

        """
        pass
        
        
    def findClassAd(self, jobspec):
        """
        _findClassAd_

        Look through list of classads and find entry with matching
        job spec id

        """
        for classad in self.classads:
            if classad['ProdAgent_JobID'] == jobspec:
                return classad
        return None


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
        dashboardInfo.read(dashboardInfoFile)

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

        if data.get('RBname', ''):
            if data['RBname'].lower().find('unknown') == -1:
              	dashboardInfo['RBname'] = data['RBname']

        # Broadcasting data
        try:
            dashboardInfo.publish(1)
            logging.debug("dashboard info sent for job %s" % jobSpecId)
        # error, cannot publish it
        except Exception, msg:
            logging.error(
                "Cannot publish dashboard information for job %s. %s" % (
                    jobSpecId, str(msg))
            )

        # update DasboardInfo file
        logging.debug("Creating dashboardInfoFile %s." % dashboardInfoFile)
        dashboardInfo.write(dashboardInfoFile)

        logging.debug("Information published in Dashboard:")
        msg = "\n - task: %s\n - job: %s" % (dashboardInfo.task,
            dashboardInfo.job)
        for key, value in dashboardInfo.items():
      	    msg += "\n - %s: %s" % (key, value)
      	logging.debug(msg)

        return


registerTracker(CondorG, CondorG.__name__)


