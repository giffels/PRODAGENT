#!/usr/bin/env python
"""
_TaskRunner_

Execution class for running a task described by a ShREEKTask instance,
and managing its execution.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: TaskRunner.py,v 1.1 2006/04/10 17:38:42 evansde Exp $"
__author__ = "evansde@fnal.gov"

import os
import time
import signal
import popen2

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
        result.append(int(item))
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
        self.logName = "task-stdout-stderr.log"
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

        args = ('>', self.logName , '2>&1') 
        
        pid = os.fork()
        
        if pid == 0:
            # child -- never returns
            self.log("TaskRunner: Spawned Child Process: %s" %os.getpid(),
                     LogStates.Dbg_med)
            os.execv(self.task.executable(), args)

        else:
            #parent
            self.process = pid
            while 1:
                (waitresult, exitCode) = os.waitpid(pid, os.WNOHANG)
                if pid == waitresult:
                    break
                
                
        if os.WIFEXITED(exitCode):
            exitCode = os.WEXITSTATUS(exitCode)
        self.log("TaskRunner.run: Child Exited %s" % exitCode,
                 LogStates.Dbg_lo)
        os.chdir(currentDir)
        return exitCode
    
            
