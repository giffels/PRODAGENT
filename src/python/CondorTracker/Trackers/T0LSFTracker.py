 #!/usr/bin/env python
"""
_T0LSFTracker_

Tracker for Tier-0 LSF  submissions


"""

import logging
import popen2
import fcntl, select, sys, os


from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker

import FwkJobRep.ReportState as ReportState


#  //
# // LSF Group name for Tier 0 jobs
#//
LSFGroupName = "/groups/tier0/reconstruction"


def makeNonBlocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
	fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)

class LSFInterface:
    """
    Bjobs API
    """

    def executeCommand(command):
        """
        _executeCommand_

        Util it execute the command provided in a popen object

        """
        logging.debug("SubmitterInterface.executeCommand:%s" % command)

        child = popen2.Popen3(command, 1) # capture stdout and stderr from command
        child.tochild.close()             # don't need to talk to child
        outfile = child.fromchild 
        outfd = outfile.fileno()
        errfile = child.childerr
        errfd = errfile.fileno()
        makeNonBlocking(outfd)            # don't deadlock!
        makeNonBlocking(errfd)
        outdata = errdata = ''
        outeof = erreof = 0
        stdoutBuffer = ""
        while 1:
            ready = select.select([outfd,errfd],[],[]) # wait for input
            if outfd in ready[0]:
                outchunk = outfile.read()
                if outchunk == '': outeof = 1
                stdoutBuffer += outchunk
                sys.stdout.write(outchunk)
            if errfd in ready[0]:
                errchunk = errfile.read()
                if errchunk == '': erreof = 1
                sys.stderr.write(errchunk)
            if outeof and erreof: break
            select.select([],[],[],.1) # give a little time for buffers to fill

        try:
            exitCode = child.poll()
        except Exception, ex:
            msg = "Error retrieving child exit code: %s\n" % ex
            msg = "while executing command:\n"
            msg += command
            logging.error("BulkSubmitterInterface:Failed to Execute Command")
            logging.error(msg)
            raise RuntimeError, msg
        
        if exitCode:
            msg = "Error executing command:\n"
            msg += command
            msg += "Exited with code: %s\n" % exitCode
            logging.error("SubmitterInterface:Failed to Execute Command")
            logging.error(msg)
            raise RuntimeError, msg
        return  stdoutBuffer

    def bjobs(groupName, specificJobId = None):
        """
        _bjobs_

        Query:
          bjobs -g groupName -j + some formatting
          If a job id is used, then the query is used to get the history for that id

        Returns:

        Dictionary of job spec id (from job name attribute) to status

        """

        logging.debug("T0LSFTracker.bjobs: Checking jobs in LSF")

        if ( specificJobId != None ) :
            output = LSFInterface.executeCommand("/usr/bin/bjobs -a -w -g " + LSFGroupName + " -J " + specificJobId)
        else :
            output = LSFInterface.executeCommand("/usr/bin/bjobs -a -w -g " + LSFGroupName)

        #logging.debug("T0LSFTracker.bjobs: %s " % output)

        statusDict = {}
        for line in output.splitlines(False)[1:]:
            linelist = line.rstrip().split()
            # might have previous version of the same job
            if ( linelist[6] in statusDict ):
                # override status if previous version failed
                if ( statusDict[linelist[6]] == 'EXIT' ):
                    statusDict[linelist[6]] = linelist[2]
            else:
                statusDict[linelist[6]] = linelist[2]

        #logging.info("T0LSFTracker.bjobs: %s" % statusDict)

        return statusDict

    bjobs = staticmethod(bjobs);
    executeCommand = staticmethod(executeCommand)


class LSFStatus:
    """
    _LSFStatus_

    Definition of LSFStatus (Not sure what these actually should be)

    """
    submitted = 'PEND'
    running = 'RUN'
    aborted = 'EXIT'
    finished = 'DONE'
    failed = 'EXIT'
    

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
        self.bjobs = LSFInterface.bjobs(LSFGroupName)
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
  
            #  // 
            # //  Job not in bjobs output, start checking other sources
            #//   First check job report
            if status == None:
                msg = "No Status entry for %s, checking job report" % subId
                logging.debug(msg)
                status = self.jobReportStatus(subId)
                
            #//
            #// if status is still None => check lsf history
            #//  for right now just check again the single job
            #//
            #if status == None:
                #status = LSFInterface.bjobs(LSFGroupName, subId).get(subId, None)

            #  //
            # // If status still None, declare job lost/failed
            #//
            if status == None:
                self.TrackerDB.jobFailed(subId)
                logging.debug("Job %s has been lost" % (subId))
                continue

            #  //
            # // Now examine the status value, not sure what these are, but act accordingly
            #//

            if status == LSFStatus.submitted:
                #  //
                # // Still submitted, nothing to be done
                #//
                logging.debug("Job %s is pending" % (subId))
                continue 
            if status in (LSFStatus.running, LSFStatus.finished):
                #  //
                # // Is running or completed already, forward to running handler
                #//
                self.TrackerDB.jobRunning(subId)
                logging.debug("Job %s is running" % (subId))
                continue
            if status in (LSFStatus.failed, LSFStatus.aborted):
                #  //
                # // Failed or Aborted
                #//
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
  
            #  // 
            # //  Job not in bjobs output, start checking other sources
            #//   First check job report
            if status == None:
                msg = "No Status entry for %s, checking job report" % runId
                logging.debug(msg)
                status = self.jobReportStatus(runId)
                
            #  //
            # // if status is still None => check lsf history
            #//
            #if status == None:    
                #status = LSFInterface.bjobs(LSFGroupName, runId).get(runId, None)

            #  //
            # // If status still None, declare job lost/failed
            #//
            if status == None:
                self.TrackerDB.jobFailed(runId)
                logging.debug("Job %s has been lost" % (runId))
                continue
            
            if status == LSFStatus.running:
                #  //
                # // Is running
                #//
                logging.debug("Job %s is still running" % (runId))
                continue
            if status == LSFStatus.finished:
                #  //
                # // Is Complete 
                #//
                self.TrackerDB.jobComplete(runId)
                logging.debug("Job %s complete" % (runId))
                continue
            if status in (LSFStatus.failed, LSFStatus.aborted):
                logging.debug("Job %s is held..." % (runId))
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
        Should return an LSF Status if a status is available, if the file cannot be found,
        return None

        """
        report = self.findJobReport(jobSpecId)
        if report == None:
            return None
        
        if ReportState.checkSuccess(report):
            return LSFStatus.finished
        return LSFStatus.failed
    

registerTracker(T0LSFTracker, T0LSFTracker.__name__)







