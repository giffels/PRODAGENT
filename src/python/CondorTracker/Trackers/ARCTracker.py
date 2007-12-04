 #!/usr/bin/env python
"""
_ARCTracker_

Tracker for Nordugrid ARC submissions


"""

import logging
import popen2
import fcntl, select, sys, os


from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker

import FwkJobRep.ReportState as ReportState


#
# The Following two functions are taken almost verbatim from
# BulkSubmitterInterface
#
def makeNonBlocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)


def executeCommand(command):
    """
    _executeCommand_

    Util it execute the command provided in a popen object

    """
    logging.debug("executeCommand: %s" % command)

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
        logging.error("executeCommand: Failed to Execute Command")
        logging.error(msg)
        raise RuntimeError, msg

    if exitCode:
        msg = "Error executing command:\n"
        msg += command
        msg += "Exited with code: %s\n" % exitCode
        logging.error("executeCommand: Failed to Execute Command")
        logging.error(msg)
        raise RuntimeError, msg
    return  stdoutBuffer


def findKey(dict,value):
    """
    Given a dictionary and a value, find the key of the value, or None, if
    no such value is found.

    """
    for i in dict.items():
        if i[1] == value:
            return i[0]
    return None
        


#  //
# //  Mapping between ARC status codes and ProdAgent status codes
#//
StatusCodes = {"ACCEPTING": "PEND",  # Job has reaced the CE
               "ACCEPTED":  "PEND",  # Job submitted but not yet processed
               "PREPARING": "PEND",  # Input files are being transferred
               "PREPARED":  "PEND",  # Transferring input files done
               "SUBMITTING":"PEND",  # Interaction with the LRMS at the CE ongoing
               "INLRMS:Q":  "PEND",  # In the queue of the LRMS at the CE
               "INLRMS:R":  "RUN",   # Running
               "INLRMS:S":  "RUN",   # Suspended
               "INLRMS:E":  "RUN",   # About to finish in the LRMS
               "INLRMS:O":  "RUN",   # Other LRMS state
               "EXECUTED":  "RUN",   # Job is completed in the LRMS
               "FINISHING": "RUN",   # Output files are being transferred
               "KILLING":   "EXIT",  # Job is being cancelled on user request
               "KILLED":    "EXIT",  # Job canceled on user request
               "DELETED":   "EXIT",  # Job removed due to expiration time
               "FAILED":    "EXIT",  # Job finished with non-zero exit code
               "FINISHED":  "DONE"}  # Job finished with zero exit code.




class ARCTracker(TrackerPlugin):
    """
    _ARCTracker_

    Poll ARC for status of jobs

    """
    def __init__(self):
        TrackerPlugin.__init__(self)
        self.jobs = {}


    def initialise(self):
        """
        _initialise_

        Retrieve data from bjobs command

        """
        self.jobs = self.getJobStatus()
        logging.debug("initialise: Retrieved status for %i Jobs" % len(self.jobs))


    def getJobStatus(self, jobSpecId = None):
        """
        Get a {jobSpecId: status} dictionary for jobSpecId, or all jobs if
        jobSpecId == None.

        """
        if jobSpecId == None:
            jobs = self.getAllJobIDs()
            ids = ""
            for id in jobs.keys():
                ids += " " + id.strip()
        else:
            ids = jobSpecId

        logging.debug("getJobStatus: Id:s to check: '%s'" % ids)
        if len(ids.strip()) > 0:
            output = executeCommand("ngstat " + ids)
        else:
            return {}

        status = {}
        for line in output.split("\n"):
            fields = line.split(":")

            if fields[0].strip() == "Job information not found":
                # There are basically two reasons why we can get a "Job
                # information not found" response; either the job was
                # submitted so recently that the middleware hasn't had time
                # to register it, or something has gone terribly wrong at
                # the CE (in which case the job is likely to be lost).

                words = line.split()
                arcId = words[4][0:-1]
                id = findKey(jobs, arcId) 

                if line.find("This job was only very recently submitted") >= 0:
                    status[id] = StatusCodes["ACCEPTING"]
                else:
                    status[id] = StatusCodes["FAILED"]
                logging.debug("Information for job %s was not found; it's assumed to be in state %s" % (id, status[id]))

            elif fields[0].strip() == "Job Name":
                id = fields[1].strip()
            elif fields[0].strip() == "Status":
                #s = fields[1].strip()
                # In case the status has a ':', e.g. INLRMS:Q, we need to
                # (re)-join them:
                #for w in fields[2:]:
                #    s += ":" + w.strip()
                s = ":".join(fields[1:]).strip()

                status[id] = StatusCodes[s]
                logging.debug("Status for job %s is %s" % (id, s))

        return status


    def getAllJobIDs(self):
        """
        Return a {jobSpecId: arcId} dictionary containing all our jobs

        """
        home = os.environ.get("HOME")
        fd = open(home + "/.ngjobs", "r")
        file = fd.readlines()
        fd.close()

        jobs = {}
        for line in file:
             # TODO: Check that there's at least 2 fields in 'line'
            arcId, jobSpecId = line.strip().split('#')[0:2]
            jobs[jobSpecId] = arcId

        return jobs



    def getJobReport(self,dir,jobSpecId):
        """
        Get the FrameworkJobReport.xml file for job 'jobSpecId' by ngcp and put
        it in firectory 'dir'.  Return the path to the local copy of the file,
        or 'None' if either the job wasn't found, or the ngcp command failed.

        """

        jobs = self.getAllJobIDs()
        if jobSpecId not in jobs.keys():
            logging.debug("getReportFromARC: Couldn't find job " + jobSpecId)
            return None

        arcId = jobs[jobSpecId]
        s = sys.system("ngcp %s/%s/FrameworkJobReport.xml %s/" % \
                        (arcId,jobSpecId,dir))
        if s != 0:
            logging.info("getReportFromARC: Report File Not Found for " \
                         + jobSpecId)
            return None

        logging.info("getReportFromARC: Report file for %s copied to %s" % \
                      (jobSpecId, dir))
        return dir + "/FrameworkJobReport.xml"




    def updateSubmitted(self, *submitted):
        """
        _updateSubmitted_

        Override to look at each submitted state job spec id provided
        and change its status if reqd.

        """
        logging.info("ARC: Submitted Count: %s" % len(submitted))
        for subId in submitted:
            status = self.jobs.get(subId, None)
  
            #  // 
            # //  Job not in getJobStatus() output, check job report
            #//
            if status == None:
                msg = "No Status entry for %s, checking job report" % subId
                logging.debug(msg)
                status = self.jobReportStatus(subId)
                
            #  //
            # // If status still None, declare job lost/failed
            #//
            if status == None:
                self.TrackerDB.jobFailed(subId)
                logging.debug("Job %s has been lost" % (subId))
                continue

            #  // 
            # // Now examine the status value, not sure what these are, but act
            #//  accordingly
            if status == "PEND":
                #  //
                # // Still submitted, nothing to be done
                #//
                logging.debug("Job %s is pending" % (subId))
                continue 
            if status in ("RUN", "DONE"):
                #  //
                # // Is running or completed already, forward to running handler
                #//
                self.TrackerDB.jobRunning(subId)
                logging.debug("Job %s is running or finished" % (subId))
                continue
            if status == "EXIT":
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
        logging.info("ARC: Running Count: %s" % len(running))
        for runId in running:

            status = self.jobs.get(runId, None)
  
            #  // 
            # //  Job not in getJobStatus output, check job report
            #//
            if status == None:
                msg = "No Status entry for %s, checking job report" % runId
                logging.debug(msg)
                status = self.jobReportStatus(runId)
                
            #  //
            # // If status still None, declare job lost/failed
            #//
            if status == None:
                self.TrackerDB.jobFailed(runId)
                logging.debug("Job %s has been lost" % (runId))
                continue
            
            if status == "RUN":
                #  //
                # // Is running
                #//
                logging.debug("Job %s is still running" % (runId))
                continue
            if status == "DONE":
                #  //
                # // Is Complete 
                #//
                self.TrackerDB.jobComplete(runId)
                logging.debug("Job %s complete" % (runId))
                continue
            if status == "EXIT":
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

        
    def findJobReport(self, jobSpecId):
        """
        _findJobReport_
        
        Given a job spec Id, find the location of the job report file if it exists.
        Return the path of the file.  If not found, return None
        
        """
        cache = self.getJobCache(jobSpecId)
        logging.debug("findJobReport, cache: " + str(cache))
        if cache == None:
            logging.debug("No JobCache found for Job Spec ID: %s" % jobSpecId)
            return None

        reportFile = "%s/FrameworkJobReport.xml" % cache
        if not os.path.exists(reportFile):
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
            return StatusCodes["FINISHED"]
        return StatusCodes["FAILED"]
    

registerTracker(ARCTracker, ARCTracker.__name__)
