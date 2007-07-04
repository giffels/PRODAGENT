 #!/usr/bin/env python
"""
_LSF_

Module to keep interactions with LSF or
handling of the LSF configuration


"""

import logging
import popen2
import fcntl, select, sys, os

from ProdAgentCore.Configuration import loadProdAgentConfiguration


def makeNonBlocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
	fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)

class LSFConfiguration:
    """
    Returns LSF group name
    """

    def getGroup():
        """
        Read LSF group name from config file
        Should I cache this ?
        """

        try:
            config = loadProdAgentConfiguration()
        except StandardError, ex:
            msg = "Error reading configuration:\n"
            msg += str(ex)
            logging.error(msg)
            raise RuntimeError, msg
                                                                                                                                                 
        if config.has_key("LSF"):

            try:
                lsfConfig = config.getConfig("LSF")
            except StandardError, ex:
                msg = "Error reading configuration for LSF:\n"
                msg += str(ex)
                logging.error(msg)
                raise RuntimeError, msg

            logging.debug("LSF Config: %s" % lsfConfig)
                                                                                                                                                 
            return lsfConfig['JobGroup']

        else:

            msg = "Configuration block LSF is missing from $PRODAGENT_CONFIG"
            logging.error(msg)

            return "/groups/tier0/default"

    getGroup = staticmethod(getGroup)


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

    def bjobs(specificJobId = None):
        """
        _bjobs_

        Query:
          If a job id is used, then the query is used to only get the job status
          History on LSF at CERN is only marginally longer then what bjobs caches
          Plus it's an expensive call, so it's not worth it

        Returns:

        Dictionary of job spec id (from job name attribute) to status

        """

        logging.debug("T0LSFTracker.bjobs: Checking jobs in LSF")

        if ( specificJobId != None ) :
            output = LSFInterface.executeCommand("/usr/bin/bjobs -a -w -g " + LSFConfiguration.getGroup() + " -J " + specificJobId)
        else :
            output = LSFInterface.executeCommand("/usr/bin/bjobs -a -w -g " + LSFConfiguration.getGroup())

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

    bjobs = staticmethod(bjobs)
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
