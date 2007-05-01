#!/usr/bin/env python
"""

_TraceUtils_

Diagnostic calls for dissecting problems at runtime

"""
import os
import popen2
import fcntl, select, sys
import StringIO

def makeNonBlocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
	fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)

def getCommandOutput(command, stdoutFile = sys.stdout,
                     stderrFile = sys.stderr):
    """
    _getCommandOutput_
    
    Run the provided command and set the process id for this instance
    
    """
    child = popen2.Popen3(command, 1) # capture stdout and stderr from command
    child.tochild.close()             # don't need to talk to child
    process = child.pid
    outfile = child.fromchild 
    outfd = outfile.fileno()
    errfile = child.childerr
    errfd = errfile.fileno()
    makeNonBlocking(outfd)            # don't deadlock!
    makeNonBlocking(errfd)
    outdata = errdata = ''
    outeof = erreof = 0
    while 1:
        ready = select.select([outfd,errfd],[],[]) # wait for input
        if outfd in ready[0]:
          outchunk = outfile.read()
          if outchunk == '': outeof = 1
          stdoutFile.write(outchunk)
        if errfd in ready[0]:
          errchunk = errfile.read()
          if errchunk == '': erreof = 1
          stderrFile.write(errchunk)
        if outeof and erreof: break
        select.select([],[],[],.1) # give a little time for buffers to fill
    
    try:
        err = child.poll()
    except Exception, ex:
        stderrFile.write("Error retrieving child exit code: %s" % ex)
        return 1
    
    return err



def strace(process, timeDuration = 5):
    """
    _strace_

    Attach strace to the process, capture output for timeDuration
    seconds and then detach.
    
    returns tuple of stdout, stderr from the strace process

    """
    command = "#!/bin/sh\n"
    command += "strace -p %s &\n" % process
    command += "TRACE_PID=$!\n"
    command += "sleep %s\n" % timeDuration
    command += "kill -2 $TRACE_PID\n"

    stdoutCapture = StringIO.StringIO()
    stderrCapture = StringIO.StringIO()
    
    getCommandOutput(command, stdoutCapture, stderrCapture)

    stdoutCapture.flush()
    stderrCapture.flush()
    
    return stdoutCapture.getvalue(),  stderrCapture.getvalue()


def gdbBacktrace(process):
    """
    _gdbBacktrace_

    Run a gdb Backtrace on a running/hung process
    
    """
    command = "#!/bin/sh\n"
    command += "gdb -p %s <<!\n" % process
    command += " bt \n"
    command += "quit\n"
    command += "y\n"
    command += "!\n"

    
    stdoutCapture = StringIO.StringIO()
    stderrCapture = StringIO.StringIO()
    
    getCommandOutput(command, stdoutCapture, stderrCapture)

    stdoutCapture.flush()
    stderrCapture.flush()
    
    return stdoutCapture.getvalue(),  stderrCapture.getvalue()

    
