#!/usr/bin/env python
"""
_Execute_

Utilities for running a command

"""

import os
import popen2
import fcntl, select, sys

from SVSuite.SVSuiteError import SVSuiteToolFailure

def makeNonBlocking(fd):
    """
    _makeNonBlocking_

    Make the file descriptor provided non-blocking

    """
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
	fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)
    

def runCommand(command, outFilter = None):
    """
    _runCommand_

    Run the command without deadlocking stdou and stderr,
    echo all output to sys.stdout and sys.stderr

    Returns the exitCode
    
    """
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
    while 1:
	ready = select.select([outfd,errfd],[],[]) # wait for input
	if outfd in ready[0]:
	    outchunk = outfile.read()
	    if outchunk == '': outeof = 1
	    sys.stdout.write(outchunk)
            if outFilter != None:
                outFilter(outchunk)
	if errfd in ready[0]:
	    errchunk = errfile.read()
	    if errchunk == '': erreof = 1
            sys.stderr.write(errchunk)
           
	if outeof and erreof: break
	select.select([],[],[],.1) # give a little time for buffers to fill
    err = child.wait()
    
    return err


def execute(command):
    """
    _execute_

    Execute the command provided, throw a SVSuiteToolFailure if returns
    non zero

    """
    exitCode = runCommand(command)
    if exitCode:
        msg = "Command exited non-zero"
        raise SVSuiteToolFailure(msg, Command = command, ExitCode = exitCode)
    return

def executeWithFilter(command, filterMethod):
    
    exitCode = runCommand(command, filterMethod)
    if exitCode:
        msg = "Command exited non-zero"
        raise SVSuiteToolFailure(msg, Command = command, ExitCode = exitCode)
    return