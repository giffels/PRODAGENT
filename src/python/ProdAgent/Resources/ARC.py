#!/usr/bin/env python

import logging
import popen2
import fcntl, select, sys, os
import re

from ProdAgent.WorkflowEntities import Job


class CommandExecutionError(Exception):
    def __init__(self, s):
        self.s = s  # Exit status of command
    def __str__(self):
        return str(self.s)


def findKey(dict, value, ifNotFound = None):
    """
    Given a dictionary and a value, return the first key found with that
    value, or None, if no such value is found.

    """
    for i in dict.items():
        if i[1] == value:
            return i[0]
    return ifNotFound
        


def makeNonBlocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)


def executeCommand(command):
    """
    Execute shell command 'command'.
    Raise a CommandExecutionError if the command has non-zero exit status.

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

    exitCode = child.poll()

    if exitCode != 0:
        msg = "Error executing command: %s" % command
        msg += "Exit code: %s\n" % exitCode
        logging.debug(msg)
        raise CommandExecutionError(exitCode)
    return stdoutBuffer


# 
# Mapping between ARC status codes and ProdAgent status codes
#
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


def jobIdMap():
    """
    Return a {jobSpecId: arcId} dictionary containing all our jobs

    Note that it's is possible that there are several jobs of the same
    name (jobSpecId:s). In that case, only the last (i.e. newest) one
    will be used.

    """
    home = os.environ.get("HOME")
    filename = home + "/.ngjobs"
    fd = open(filename, "r")
    file = fd.readlines()
    fd.close()

    jobs = {}
    for i in range(len(file)):
        line = file[i]
        fields = line.strip().split('#')

        if len(fields) < 2:
            logging.warning("Somethings funny with line %i in file %s" % (i+1, fname))
            continue

        arcId, jobSpecId = fields[0:2]
        jobs[jobSpecId] = arcId
        logging.debug("jobIdMap: %s: %s" % (jobSpecId, arcId))

    return jobs


noInfo = {}  # jobSpecId:s we haven't gotten any info for for a while

def incNoInfo(arcId):
    noInfo[arcId] = noInfo.get(arcId, 0) + 1

def clearNoInfo(arcId):
    if noInfo.has_key(arcId): del noInfo[arcId]

def getNoInfo(arcId):
    return noInfo.get(arcId, 0)


class ARCJob:
     #
     #   FIXME: This could be turned into a dictionary that extends the
     #   information from Job.get() with some ARC specific information.
     #

    def __init__(self, jobIds = None, arcId = None, jobSpecId = None, status = None, CEName = None):
        """
        Note that either arcId or jobSpecId has to be provided. If either
        is missing, we'll try to figure it out using the other.  
        
        self.jobSpecId should allways end up being set to a string (but
        could be an empty string!); self.arcId, however, might end up as
        None in the special case that no such ARC job actually exist (ought
        to be a rare condition, but is in principle possible).

        """
        assert arcId or jobSpecId

        if not jobIds: jobIds = jobIdMap()
            
        if arcId:
            self.arcId = arcId
        else:
            self.arcId = jobIds.get(jobSpecId, None)

        if jobSpecId:
            self.jobSpecId = jobSpecId
        else:
            self.jobSpecId = findKey(jobIds, arcId)

        assert self.jobSpecId

        self.status = status

        if CEName:
            self.CEName = CEName
        elif self.arcId:
            # Extract CEName from the arcId.  Assume the arcId has the form
            # protocol://<CEname>[:port]/what/ever
            # FIXME: What happens if arcId hasn't that form?
            s = re.sub('^\w+://', '', arcId)       
            self.CEName = re.sub('[:/].*$', '', s)
        else:
            self.CEName = None

        if Job.exists(self.jobSpecId) > 0:
            self.jobinfo = Job.get(self.jobSpecId)
        else:
            self.jobinfo = {}
            

def execNgstat(ids):
    msg = "execNgstat: Id:s to check:\n -> " + ids.strip().replace(" ", "\n -> ")
    logging.debug(msg)

    if not ids.strip(): return ""

    try:
        return executeCommand("ngstat " + ids)
    except CommandExecutionError, s:
        #msg = "Command 'ngstat %s' exited with exit status %s" % (ids, str(s))
        raise RuntimeError, msg
            

def getJobs():
    """
    Return a list of ARCJobs

    """

    jobIds = jobIdMap()
    ids = ""
    n = 0
    output = ""
    for id in jobIds.keys():
        if id.find(" ") >= 0:
            id = '"' + id.strip() + '"'
        ids += id.strip() + " "
        n += 1
        if n >= 50:
            output += execNgstat(ids)
            ids = ""
            n = 0

    if n > 0:
        output += execNgstat(ids)

    jobs = []
    for line in output.split("\n"):
        fields = line.split(":")

        if fields[0].strip() == "Job information not found":
            words = line.split()
            arcId = words[4][0:-1]

            if line.find("This job was only very recently submitted") >= 0:
                s = "ASSUMED_NEW"
            else:
                # Assume the server is just too busy to respond, and
                # that the job is happily running, unless we've gotten
                # many "Job information not found" in a row, in which
                # case we assume something bad has happened.
                logging.debug("NoInfo for job %s is %s\n" % (id, str(getNoInfo(arcId))))
                if getNoInfo(arcId) < 10:
                    s = "ASSUMED_ALIVE"
                    incNoInfo(arcId)
                else:
                    s = "ASSUMED_LOST"
            status = StatusCodes[s]

            try:
                j = ARCJob(jobIds = jobIds, arcId = arcId, status = status)
            except AssertionError:
                # We could end up here e.g. if a job is removed between the
                # jobIdMap() call in the beginning of this function, and the
                # ARCJob() call above. In short, it _shouldn't_ happen.
                logging.warning("Something is seriously wrong with arcId = %s! Job Removed manually?" % arcId)
                continue

            jobs.append(j)

            msg = "Information for job %s/%s was not found;\n" % (str(j.jobSpecId), arcId)
            msg += " -> it's assigned the state " + s
            logging.debug(msg)

        elif fields[0].strip() == "Malformed URL":
            # "Malformed URL" is something we might get for
            # non-existent (e.g. lost) jobs.
            jobSpecId = fields[1].strip()
            status = StatusCodes["ASSUMED_LOST"] 

            jobs.append(ARCJob(jobIds = JobIds, jobSpecID = jobSpecId, status = status))

            msg = "Malformed URL: "
            msg += "Status for job %s is ASSUMED_LOST" % id
            logging.warning(msg)

        #
        # With special cases taken care of above, we are left with "normal"
        # jobs. They are assumed to have the format
        #
        # Job <arcId>
        #   Job Name: <jobSpecId>
        #   Status: <status>
        #
        # Additional lines may exist, but we'll ignore them. 
        #
        # Note that while ARC jobs in general might be missing the 'Job
        # Name' line, it shouldn't happen here.
        # 
        elif line.find("Job ") == 0:
            arcId = line.split()[1]
            jobSpecId = None

        elif fields[0].strip() == "Job Name":
            jobSpecId = fields[1].strip()

        elif fields[0].strip() == "Status":
            if not jobSpecId:
                logging.warning("Job without name (arcId = %s) found and ignored" % arcId)
                # FIXME: What if we haven't gotten any arcId for the job
                # either?
                continue

            s = ":".join(fields[1:]).strip()
            status = StatusCodes[s]
            jobs.append(ARCJob(jobIds=jobIds, arcId=arcId, jobSpecId=jobSpecId, status=status))

            clearNoInfo(arcId)

            logging.debug("Status for job %s is %s" % (jobSpecId, s))

    return jobs


def getJobsLite():
    """
    Return a list of ARCJobs, but only including information that can be
    extracted from the filen ~/.ngjobs. (IOW, no job status)

    """

    jobs = []
    jobIds = jobIdMap()
    for (jobSpecId, arcId) in jobIds.items():
        if len(jobSpecId.strip()) > 0:
            j = ARCJob(jobIds = jobIds, arcId = arcId, jobSpecId = jobSpecId)
            jobs.append(j)
    return jobs
