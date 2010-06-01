 #!/usr/bin/env python
"""
_CondorDirect_

Tracker for CondorDirect submissions


"""

import logging
import os
from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker
from CondorTracker.Trackers.CondorLog import readCondorLog

from ResourceMonitor.Monitors.CondorQ import condorQ


import ProdAgent.WorkflowEntities.Utilities as WEUtil
    
def extractReportFromStdout(stdoutFile):
    """
    _extractReportFromStdout_

    Search for and extract if present, the FrameworkJobReport content
    from the stdout

    """
    reportContent = ""
    startBuffering = False
    handle = open(stdoutFile, 'r')
    for line in handle.readlines():
        if line.startswith("===CUT HERE==="):
            startBuffering = True
            continue
        if not startBuffering: continue
        reportContent += line

    if startBuffering == False:
        return None
    
    return reportContent


class CondorDirect(TrackerPlugin):
    """
    _CondorDirect_

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
        logging.info("CondorDirect: Submitted Count: %s" % len(submitted))
        for subId in submitted:
            status = None
            classad = self.findClassAd(subId)
            if classad == None:
                msg = "No Classad for %s, checking condor log" % subId
                logging.debug(msg)
                cache = self.getJobCache(subId)
                if cache == None:
                    msg = "Unable to find cache dir for job %s\n" % subId
                    msg += "Declaring job aborted"
                    logging.warning(msg)
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
                    self.TrackerDB.jobFailed(subId)
                    continue
                
                condorLog = readCondorLog(condorLogFile)
                if condorLog == None:
                    msg = "Cannot read condor log file:\n%s\n" % condorLogFile
                    msg += "Not an XML log??\n"
                    msg += "Declaring job aborted"
                    logging.warning(msg)
                    self.TrackerDB.jobFailed(subId)
                    continue
                #  //
                # // We have got the log file and computed an exit status 
                #//  for it.
                status = condorLog.condorStatus()
                
            else:
                status = classad['JobStatus']
                

            if status == 1:
                logging.debug("Job %s is pending" % (subId))
                continue 
            if status == 2:
                #  //
                # // Is running
                #//
                self.TrackerDB.jobRunning(subId)
                logging.debug("Job %s is running" % (subId))
                continue
            if status == 4:
                #  //
                # // Is Complete -- but we want to forward to UpdateRunning first
                #//
                self.TrackerDB.jobRunning(subId)
                logging.debug("Job %s complete" % (subId))
                continue
            if status == 5:
                #  //
                # // Held 
                #//
                logging.debug("Job %s is held..." % (subId))
                self.TrackerDB.jobFailed(subId)
#                self.TrackerDB.killJob(subId)
                command="condor_rm %s " % subId
                logging.debug("Removing job from queue...")
                logging.debug("Executing %s " % command)
                os.system(command)
                continue
            if status in (3, 6):
                #  //
                # // Error or Removed
                #//
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
        logging.info("CondorDirect: Running Count: %s" % len(running))
        for runId in running:
            classad = self.findClassAd(runId)
            if classad == None:
                msg = "No Classad for %s, checking condor log" % runId
                logging.debug(msg)
                cache = self.getJobCache(runId)
                if cache == None:
                    msg = "Unable to find cache dir for job %s\n" % runId
                    msg += "Declaring job aborted"
                    logging.warning(msg)
                    self.TrackerDB.jobFailed(runId)
                    continue
                # first check if shortened version exists...
                condorLogFile = "%s/condor.log" % cache
                if not os.path.exists(condorLogFile):
                   condorLogFile = "%s/%s-condor.log" % (cache, runId)
                if not os.path.exists(condorLogFile):
                    msg = "Cannot find condor log file:\n%s\n" % condorLogFile
                    msg += "Declaring job aborted"
                    logging.warning(msg)
                    self.TrackerDB.jobFailed(runId)
                    continue
                
                condorLog = readCondorLog(condorLogFile)
                if condorLog == None:
                    msg = "Cannot read condor log file:\n%s\n" % condorLogFile
                    msg += "Not an XML log??\n"
                    msg += "Declaring job aborted"
                    logging.warning(msg)
                    self.TrackerDB.jobFailed(runId)
                    continue
                #  //
                # // We have got the log file and computed an exit status 
                #//  for it.
                classad = {}
                classad['JobStatus'] = condorLog.condorStatus()
                classad['ClusterId'] = condorLog['Cluster']
                
            status = classad['JobStatus']
            clusterId = classad['ClusterId']

            if status in (3, 6):
                #  //
                # // Removed or Error
                #//
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
                self.TrackerDB.jobFailed(runId)
            if status == 2:
                logging.debug("Job %s is running, ClusterId=%s " % (runId,clusterId))
            if status == 4:
                logging.debug("Job %s is complete, ClusterId=%s" % (runId,clusterId))
                self.TrackerDB.jobComplete(runId)
            if status == 1:
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
        caches = WEUtil.getJobCache(*complete)
        for compId in complete:
            summary += " -> %s\n" % compId
            #  //
            # // Lookup the job cache directory for the job, if 
            #//  the FrameworkJobReport.xml doesnt exist, search
            #  //for it in the job stdout
            # //
            #//
            if not caches.has_key(compId):
                msg = "Unable to find cache for complete job: %s" % compId
                logging.warning(compId)
                continue
            cacheDir = caches[compId]
            fwkRep = os.path.join(cacheDir, "FrameworkJobReport.xml")
            if os.path.exists(fwkRep):
                msg = "Job Report exists for completed job: %s" % compId
                logging.debug(msg)
                continue
            msg = "Job Report not found for completed job: %s" % compId
            msg += "Checking stdout file..."
            logging.debug(msg)
            condorLogFile = "%s/condor.out" % cacheDir
            if not os.path.exists(condorLogFile):
                msg = "Cannot find stdout file for completed job: %s" % compId
                logging.warning(msg)
                continue
            
            reportContent = extractReportFromStdout(condorLogFile)
            if reportContent != None:
                msg = "Extracted report from stdout for job: %s" % compId
                logging.debug(msg)
                handle = open(fwkRep, 'w')
                handle.write(reportContent)
                handle.close()
                continue
            msg = "Unable to extract report from \n%s \nfor job %s" % (
                condorLogFile, compId)
            logging.warning(msg)
            

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

        
    

registerTracker(CondorDirect, CondorDirect.__name__)







