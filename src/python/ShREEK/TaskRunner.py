#!/usr/bin/env python
"""
_TaskRunner_

Execution class for running a task described by a ShREEKTask instance,
and managing its execution.

"""
__version__ = "$Revision: 1.3 $"
__revision__ = "$Id: TaskRunner.py,v 1.3 2006/06/05 20:50:33 evansde Exp $"
__author__ = "evansde@fnal.gov"

import os
import time
import signal
import popen2
import fcntl, select, sys

from ShLogger.LogInterface import LogInterface
from ShLogger.LogStates import LogStates
from ShREEK.ShREEKException import ShREEKException

def findChildProcesses(pid):
    """
    _findChildProcesses_

    Given a PID, find all the direct child processes of that PID using ps
    command

    """
    pop = popen2.Popen4(
            "/bin/ps --no-heading --ppid %s -o pid" % pid
            )
    while pop.poll() == -1:
        exitCode = pop.poll()
    exitCode = pop.poll()
    output = pop.fromchild.read().strip()
    result = []
    for item in output.split():
        try:
            value = int(item)
        except ValueError:
            continue
        result.append(value)
    return result


class ChildProcs(list):
    """
    _ChildProcs_

    Child process accumulator to recursively find all descendant processes of
    a given PID

    """
    def __init__(self, process):
        list.__init__(self)
        self.append(process)

    def __call__(self):
        """
        _operator()_

        When called generate the list of children
        """
        self.getChildren(self[0])
        
        
        
    def getChildren(self, pid):
        """
        recursive call to get children for a process
        """
        childProcs = findChildProcesses(pid) 
        for child in childProcs:
            self.append(child)
            self.getChildren(child)
        return 

    
class TaskRunner(LogInterface):
    """
    _TaskRunner_

    """
    def __init__(self, shreekTask):
        LogInterface.__init__(self)
        self.task = shreekTask
        self.logName = "%s-stdout.log" % self.task.executable()
        self.process = -1
        

    def findProcesses(self):
        """
        _findProcesses_

        Look for any processes started by the child process
        """
        procFinder = ChildProcs(self.process)
        procFinder()
        return procFinder[1:]
        
        
        
    def killTask(self):
        """
        _killTask_

        Set the flag to kill the runnning task and any child processes
        that it started
        """
        self.log("TaskRunner.killTask called", LogStates.Info)
        if self.process > -1:
            procList = self.findProcesses()
            self.log("TaskRunner: Child Process: %s" % self.process,
                     LogStates.Dbg_lo)
            for process in procList:
                self.log("Terminating Process: %s" % process,
                         LogStates.Dbg_med)
                try:
                    os.kill(int(process), signal.SIGTERM)
                except OSError:
                    pass
            time.sleep(2)
            procList = self.findProcesses()
            for process in procList:
                self.log("Killing Process: %s" % process, LogStates.Dbg_med)
                try:
                    os.kill(int(process), signal.SIGKILL)
                except OSError:
                    procList.remove(process)
            self.log("Killing Child Process: %s" % self.process,
                     LogStates.Dbg_lo) 
            try:
                os.kill(self.process, signal.SIGTERM)
            except OSError:
                pass
        return
        

    def evalStartControlPoint(self, exeThreadRef):
        """
        _evalStartControlPoint_

        Evaluate the start control point for the ShREEKTask

        """
        thisDir = os.getcwd()
        if not os.path.exists(self.task.directory()):
            msg = "Task Directory Not Found:\n"
            msg += "%s\n" % self.task.directory()
            msg += "Unable to execute ControlPoint for task: %s\n" % (
                self.task.taskname(),
                )
            raise ShREEKException(msg, ClassInstance = self,
                                  MissingDir = self.task.directory(),
                                  TaskName = self.task.taskname())
        os.chdir(self.task.directory())
        controlPoint = self.task.startControlPoint
        controlPoint.executionMgr = exeThreadRef
        try:
            controlPoint()
        except ShREEKException, ex:
            ex.message += "Error Evaluating Control Point:\n"
            ex.message += "Task:\n"
            ex.message += str(self.task)
            raise ex
        os.chdir(thisDir)
        return

    def evalEndControlPoint(self, exeThreadRef):
        """
        _evalEndControlPoint_

        Evaluate the post task control point for the ShREEKTask

        """
        thisDir = os.getcwd()
        if not os.path.exists(self.task.directory()):
            msg = "Task Directory Not Found:\n"
            msg += "%s\n" % self.task.directory()
            msg += "Unable to execute ControlPoint for task: %s\n" % (
                self.task.taskname(),
                )
            raise ShREEKException(msg, ClassInstance = self,
                                  MissingDir = self.task.directory(),
                                  TaskName = self.task.taskname())
        os.chdir(self.task.directory())
        controlPoint = self.task.endControlPoint
        controlPoint.executionMgr = exeThreadRef
        try:
            controlPoint()
        except ShREEKException, ex:
            ex.message += "Error Evaluating End Control Point:\n"
            ex.message += "Task: %s\n " % self.task.taskname()
            ex.message += str(self.task)
            raise ex
        os.chdir(thisDir)
        return


    def run(self):
        """
        _run_

        Execute the actual task in its own subprocess and track any
        processes spawned by it.

        """
        self.log("TaskRunner.run: %s" % self.task.taskname, LogStates.Dbg_lo)
        self.log("TaskRunner.run: Dir=%s Exe=%s" % (self.task.directory(),
                                                    self.task.executable()),
                 LogStates.Dbg_med)
        currentDir = os.getcwd()
        if not os.path.exists(self.task.directory()):
            msg = "Task Directory Not Found:\n"
            msg += "%s\n" % self.task.directory()
            msg += "Unable to execute task: %s\n" % (
                self.task.taskname(),
                )
            raise ShREEKException(msg, ClassInstance = self,
                                  MissingDir = self.task.directory(),
                                  TaskName = self.task.taskname())
        os.chdir(self.task.directory())

        #args = ("  ",)
        
        #pid = os.fork()
        
        #if pid == 0:
        #    # child -- never returns
        #    self.log("TaskRunner: Spawned Child Process: %s" %os.getpid(),
        #             LogStates.Dbg_med)
        #    os.execv(self.task.executable(), args)

        #else:
        #    #parent
        #    self.process = pid
        #    while 1:
        #        (waitresult, exitCode) = os.waitpid(pid, os.WNOHANG)
        #        if pid == waitresult:
        #            break

        #print exitCode
        
        #if os.WIFEXITED(exitCode):
        #    exitCode = os.WEXITSTATUS(exitCode)
        
        command = "./%s | tee %s" % (self.task.executable(), self.logName)
        
        exitCode = getCommandOutput(command)
        

        self.log("TaskRunner.run: Child Exited %s" % exitCode,
                 LogStates.Dbg_lo)
        os.chdir(currentDir)
        return exitCode
    
            
def makeNonBlocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
	fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)
    

def getCommandOutput(command):
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
	if errfd in ready[0]:
	    errchunk = errfile.read()
	    if errchunk == '': erreof = 1
            sys.stderr.write(errchunk)
	if outeof and erreof: break
	select.select([],[],[],.1) # give a little time for buffers to fill

    try:
        err = child.poll()
    except Exception, ex:
        sys.stderr.write("Error retrieving child exit code: %s" % ex)
        return 1
    
    return err

