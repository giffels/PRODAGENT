#!/usr/bin/env python

import logging
import popen2
import fcntl, select, sys, os
import re


class CommandExecutionError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return str(self.msg)


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


def executeNgCommand(command, proxyRetry = True):
    """
    Execute shell command 'command'. If proxyRetry = True and 'command' fails with an error
    message like 'Could not determine location of a proxy certificate' or
    'The proxy has expired', (re-)initialise a proxy and retry.  (Primarily
    meant for ng* commands, but should work for any shell commands.)

    """

    logging.debug("executeNgCommand: %s" % command)

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
        msg = "Error retrieving child exit code: %s\n" % str(ex)
        msg = "while executing command:\n"
        msg += command
        logging.error("executeNgCommand: Failed to Execute Command")
        logging.error(msg)
        raise CommandExecutionError(msg)

    if exitCode:
        if proxyRetry and (stdoutBuffer.find("Could not determine location of a proxy certificate") >= 0 \
                           or stdoutBuffer.find("The proxy has expired") >= 0):
            executeNgCommand("grid-proxy-init")
            return executeNgCommand(command, False)
        msg = "Error executing command:\n"
        msg += command
        msg += "Exited with code: %s\n" % exitCode
        logging.error("executeNgCommand: Failed to Execute Command")
        logging.error(msg)
        raise CommandExecutionError(msg)
    return  stdoutBuffer


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
        fields = line.strip().split('#')[0:2]

        if len(fields) < 2:
            logging.warning("Somethings funny with line %i in file %s" % (i+1, fname))
            continue

        arcId, jobSpecId = fields[0:2]
        jobs[jobSpecId] = arcId
        logging.debug("jobIdMap: %s: %s" % (jobSpecId, arcId))

    return jobs


noInfo = {}  # jobSpecId:s we haven't gotten any info for for a while

def incNoInfo(jobSpecId):
    noInfo[jobSpecId] = noInfo.get(jobSpecId, 0) + 1

def clearNoInfo(jobSpecId):
    if noInfo.has_key(jobSpecId): del noInfo[jobSpecId]

def getNoInfo(jobSpecId):
    return noInfo.get(jobSpecId, 0)


class ARCJob:

    def __init__(self, arcId = None, jobSpecId = None, status = None, CEName = None,
                 jobType = None):
        """
        Note that either arcId or jobSpecId has to be provided. If either
        is missing, we'll try to figure it out using the other.  
        
        self.jobSpecId should allways end up being set to a string (but
        could be an empty string!); self.arcId, however, might end up as
        None in the special case that no such ARC job actually exist (ought
        to be a rare condition, but is in principle possible).

        """
        assert arcId or jobSpecId
            
        if arcId:
            self.arcId = arcId
        else:
            jobIds = jobIdMap()
            self.arcId = jobIds.get(jobSpecId, None)

        if jobSpecId:
            self.jobSpecId = jobSpecId
        else:
            jobIds = jobIdMap()
            self.jobSpecId = findKey(jobIds, arcId)

        assert self.jobSpecId

        self.status = status

        if jobType:
            self.jobType = jobType
        else:
            # FIXME: Are these assumptions on job naming conventions allways
            # true?
            if self.jobSpecId.find("mergejob") >= 0:
                self.jobType = "Merge"
            elif self.jobSpecId.find("cleanup") >= 0:
                self.jobType = "CleanUp"
            else:
                self.jobType = "Processing"

        if CEName:
            self.CEName = CEName
        elif self.arcId:
            # Extract CEName from the arcId.  Assume the arcId has the form
            # protocol://<CEname>[:port]/what/ever, where CEName may contain
            # characters [a-zA-Z0-9-_.]
            # FIXME: What happens if arcId hasn't that form?
            s = re.sub('^\w+://', '', arcId)       
            self.CEName = re.sub('[:/].*$', '', s)
        else:
            self.CEName = None

            


def getJobs():
    """
    Return a list of ARCJobs

    """

    jobIds = jobIdMap()
    ids = ""
    for id in jobIds.keys():
        ids += id.strip() + " "

    msg = "getJobs: Id:s to check:\n -> " + ids.strip().replace(" ", "\n -> ")
    logging.debug(msg)

    if ids.strip():
        output = executeNgCommand("ngstat " + ids)
    else:
        return []

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
                if getNoInfo(id) < 10:
                    s = "ASSUMED_ALIVE"
                    incNoInfo(id)
                else:
                    s = "ASSUMED_LOST"
            status = StatusCodes[s]

            try:
                j = ARCJob(arcId = arcId, status = status)
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

            jobs.append(ARCJob(jobSpecID = jobSpecId, status = status))

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
            jobs.append(ARCJob(arcId = arcId, jobSpecId = jobSpecId, status = status))

            clearNoInfo(jobSpecId)

            logging.debug("Status for job %s is %s" % (jobSpecId, s))

    return jobs

