#!/usr/bin/env python
"""
_TaskRunner_

Execution class for running a task described by a ShREEKTask instance,
and managing its execution.

"""
__version__ = "$Revision: 1.9 $"
__revision__ = "$Id: TaskRunner.py,v 1.9 2007/06/27 17:38:11 evansde Exp $"
__author__ = "evansde@fnal.gov"

import os
import time
import signal
import popen2
import fcntl, select, sys


from ShREEK.ShREEKException import ShREEKException


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
    

def findChildProcesses(pid):
    """
    _findChildProcesses_

    Given a PID, find all the direct child processes of that PID using ps
    command

    """
    procs={}
    procs=findChildProcessnames(pid)

    result=[]

    for thing in procs.keys():
       result.append(thing)

    return result   




def findChildProcessnames(pid):
    """
    _findChildProcesses_
    Given a PID, find all the direct child processes of that PID using ps
    command, returning a dictionary of names+proc numbers
    """

    command = "/bin/ps -e --no-headers -o pid -o ppid -o fname"

    output = executeCommand(command)
    #print "ps output: %s" % output

  
    pieces = []
    procnames = {}
    for line in output.split("\n"):
      pieces= line.split()
      try: 
        value=int(pieces[1])
      except Exception,e:
        #print "trouble interpreting ps output %s: \n %s" % (e,pieces)
        continue
      if value==pid:
        try:
          job=int(pieces[0])
        except ValueError,e:
          #print "trouble interpreting ps output %s: \n %s" % (e,pieces[0])
          continue
#        result.append(job)
        procnames[job]=pieces[2]
      
#    for item in output.split():
#        try:
#            value = int(item)
#        except ValueError,e:
#            print "trouble interpreting ps output %s: \n %s \n" % (e,item,output)
#            continue
#        result.append(value)
    return procnames

    

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
            print "Child Process found: %s" % child
            self.append(child)
            self.getChildren(child)
        return 



    
class TaskRunner:
    """
    _TaskRunner_

    """
    def __init__(self, shreekTask):
        self.task = shreekTask
        self.logName = "%s-stdout.log" % self.task.executable()
        self.errName = "%s-stderr.log" % self.task.executable()
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
        print "TaskRunner.killTask called"
        if self.process > -1:
            procList = self.findProcesses()
            for process in procList:
                print "Sending SIGTERM to process: %s " % process
                try:
                    os.kill(int(process), signal.SIGTERM)

                except OSError:
                    pass
            time.sleep(2)
            procList = self.findProcesses()
            for process in procList:
                print "Sending SIGKILL to process: %s " % process
                try:
                    os.kill(int(process), signal.SIGKILL)
                except OSError,e:
                    print "SIGKILL error: %s, removing process from list..." % e 
                    procList.remove(process)
            try:
                os.kill(self.process, signal.SIGTERM)
            except OSError:
                pass
        else:
            print "self.process <= -1"
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
        if not self.task.active():
            msg = "Task %s/%s is inactive and will not be executed" % (
                self.task.directory(),
                self.task.taskname()
                )
            print msg
            return 0
        
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
        
        command = "(((./%s | tee %s) 3>&1 1>&2 2>&3 | tee %s) " % (
            self.task.executable(), self.logName, self.errName,
            )
        command += "3>&1 1>&2 2>&3)"
        
        exitCode = self.getCommandOutput(command)
        os.chdir(currentDir)
        return exitCode
    
            
    
    
    def getCommandOutput(self, command):
        """
        _getCommandOutput_

        Run the provided command and set the process id for this instance
        
        """
        child = popen2.Popen3(command, 1) # capture stdout and stderr from command
        child.tochild.close()             # don't need to talk to child
        self.process = child.pid
        print "My process number is: %s" % self.process
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

