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
import CondorTracker.CondorTrackerDB as TrackerDB

from ProdAgent.WorkflowEntities import Job

import ProdCommon.FwkJobRep.ReportState as ReportState


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
               "FINISHED":  "DONE",  # Job finished with zero exit code.

               # A few status codes of our own, used in uncertain cases
               # ("Job information not found" and similar)
               "ASSUMED_NEW":   "PEND",  # Too new to be known by ARC.
               "ASSUMED_ALIVE": "RUN",   # Appears to be lost, but we still 
                                         # have hope
               "ASSUMED_LOST":  "EXIT"}  # We've lost hope.




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
                ids += id.strip() + " "

        else:
            ids = jobSpecId

        msg = "getJobStatus: Id:s to check:\n -> " + ids.strip().replace(" ", "\n -> ")
        logging.debug(msg)

        if len(ids.strip()) > 0:
            output = executeCommand("ngstat " + ids)
        else:
            return {}

        status = {}
        for line in output.split("\n"):
            fields = line.split(":")

            if fields[0].strip() == "Job information not found":
                words = line.split()
                arcId = words[4][0:-1]
                id = findKey(jobs, arcId) 

                if line.find("This job was only very recently submitted") >= 0:
                    s = "ASSUMED_NEW"
                else:
                    # Assume the server is just too busy to respond, and
                    # that the job is happily running, unless we've gotten
                    # many "Job information not found" in a row, in which
                    # case we assume something bad has happened.
                    if noInfo.get(id, 0) < 10:
                        s = "ASSUMED_ALIVE"
                        noInfo[id] = noInfo.get(id, 0) + 1
                    else:
                        s = "ASSUMED_LOST"

                status[id] = StatusCodes[s]
                msg = "Information for job %s was not found;\n" % id
                msg += " -> it's assigned the state " + s
                logging.debug(msg)

            elif fields[0].strip() == "Malformed URL":
                # "Malformed URL" is something we might get for
                # non-existent (e.g. lost) jobs.
                id = fields[1].strip()
                status[id] = StatusCodes["ASSUMED_LOST"] 
                msg = "Malformed URL: "
                msg += "Status for job %s is ASSUMED_LOST" % id
                logging.debug(msg)

            elif fields[0].strip() == "Job Name":
                id = fields[1].strip()

            elif fields[0].strip() == "Status":
                s = ":".join(fields[1:]).strip()
                if s in StatusCodes.keys(): 
                    status[id] = StatusCodes[s]
                    if noInfo.has_key(id): del noInfo[id]
                logging.debug("Status for job %s is %s" % (id, s))

        return status


    def getAllJobIDs(self):
        """
        Return a {jobSpecId: arcId} dictionary containing all our jobs

        Note that it's is possible that there are several jobs of the same
        name (jobSpecId:s). In that case, only the last (i.e. newest) one
        will be used.

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



    def getJobReport(self,localDir,jobSpecId):
        """
        Get the FrameworkJobReport.xml file for job 'jobSpecId' by ngcp and put
        it in firectory 'dir'.  Return the path to the local copy of the file,
        or 'None' if either the job wasn't found, or the ngcp command failed.

        """

        jobs = self.getAllJobIDs()
        if jobSpecId not in jobs.keys():
            logging.debug("getJobReport: Couldn't find job " + jobSpecId)
            return None

        #  //
        # // Get the FrameworkJobReport.xml file, supposed to reside in
        #//  arcId/workflow_id/FrameworkJobReport.xml.  

        arcId = jobs[jobSpecId]
        subDir = Job.get(jobSpecId)["workflow_id"]
        ngcp = "ngcp %s/%s/FrameworkJobReport.xml %s/" % (arcId,subDir,localDir)
        logging.debug("getJobReport: " + ngcp)
        s = os.system(ngcp)
        if s != 0:
            logging.warning("getJobReport: Report File Not Found for " \
                            + jobSpecId)
            return None

        logging.debug("getJobReport: Report file for %s copied to %s" % \
                      (jobSpecId, localDir))

        # Let's get a few additional files as well; they can be useful for
        # tracking down errors. 
        s = os.system("ngcp %s/%s/run.log %s/" % (arcId,subDir,localDir))
        s = os.system("ngcp %s/output %s/" % (arcId,localDir))
        s = os.system("ngcp %s/errors %s/" % (arcId,localDir))
        #s = os.system("ngcp -r 5 %s/ %s/" % (arcId,localDir))

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
        for id in running:

            status = self.jobs.get(id, None)
  
            #  // 
            # //  Job not in getJobStatus output, check job report
            #//
            if status == None:
                msg = "No Status entry for %s, checking job report" % id
                logging.debug(msg)
                status = self.jobReportStatus(id)
                
            #  //
            # // If status still None, declare job lost/failed
            #//
            if status == None:
                self.TrackerDB.jobFailed(id)
                logging.debug("Job %s has been lost" % (id))
                continue
            
            if status == "RUN":
                #  //
                # // Is running
                #//
                logging.debug("Job %s is still running" % (id))
                continue
            if status == "DONE":
                #  //
                # // Is Complete 
                #//
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
            os.system("ngclean " + id)
            summary += " -> %s\n" % id
            if noInfo.has_key(id): del noInfo[id]
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
            if noInfo.has_key(id): del noInfo[id]
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
            return StatusCodes["FINISHED"]
        return StatusCodes["FAILED"]


registerTracker(ARCTracker, ARCTracker.__name__)

noInfo = {}
