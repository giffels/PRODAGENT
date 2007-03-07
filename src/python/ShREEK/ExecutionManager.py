#!/usr/bin/env python
"""
_ExecutionManager_

Task execution manager for running a series of executable tasks represented
by a tree of ShREEKTasks

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ExecutionManager.py,v 1.1 2006/04/10 17:38:42 evansde Exp $"
__author__ = 'evansde@fnal.gov'




from ShREEK.ShREEKException import ShREEKException
from ShREEK.TaskRunner import TaskRunner

class ExecutionManager:
    """
    _ExecutionManager_
    
    Task execution manager for running a series of executable tasks
    Invoked by the ShREEK Executor, this thread executes the ShREEKTasks
    with callbacks to the monitoring system.
    
    """
    def __init__(self):
        self.jobID = None
        self.taskTree = None
        self.monitorThread = None
        self._Finished   =  False
        self._ExitCode = 0
        self._NextTask = None
        self.currentTask = None

    def jobComplete(self):
        """
        _jobComplete_

        Used to notify ExecutionThread of normal completion
        of a job. 
        """
        self._Finished = True
        return

    def exitCode(self):
        """
        _exitCode_

        Retrieve the value of the exit Code to be used to exit the main
        process invoking ShREEK.

        """
        return self._ExitCode
        
    def killjob(self, shreekExitCode = 0):
        """
        _killjob_
        
        kill the current task and finish the job.
        Optionally, the exit code for the ShREEK process can be provided
        if required, so that failed jobs can be clearly indicated to
        external wrapppers.

        """
        self._Finished = True
        self._ExitCode = shreekExitCode
        if self.monitorThread != None:
            self.monitorThread.notifyKillJob()
        self.currentTask.killTask()
        return

    def killtask(self):
        """
        _killtask_
        
        kill the current task,  not the job,
        and proceed to the next task
        """
        if self.monitorThread != None:
            self.monitorThread.notifyKillTask()
        self.currentTask.killTask()
        return

    def setNextTask(self, taskname):
        """
        _setNextTask_

        Set the name of the next ShREEKTask to be executed.
        This method can be used by control points to influence
        the flow of task execution

        """
        self._NextTask = taskname
        return
        
    def run(self):
        """
        _run_

        Execute the ShREEKTask tree, Start with the top node
        of the ShREEKTask tree and start processing it in a recursive
        descent pattern. ControlPoints may alter the flow during processing
        depending on how they are configured
        
        """
        print "Starting Job Execution"
        print "JobID: %s" % self.jobID
        if self.monitorThread != None:
            self.monitorThread.notifyJobStart()
            
        self.executeTask(self.taskTree)
        
        print "Finished Job"
        if self.monitorThread != None:
            self.monitorThread.notifyJobEnd()
            
        return


    def executeTask(self, task):
        """
        _executeTask_

        Execute a ShREEKTask, including its control points.
        If a ControlPoint sets the next task name, that task is executed,
        otherwise, recursive iteration through children is performed

        """
        if self._Finished:
            return
        
        self._NextTask = None
        taskRunner = TaskRunner(task)
        self.currentTask = taskRunner
        print "Starting Task Execution: %s" % task.taskname()
        taskRunner.evalStartControlPoint(self)
        if self._NextTask != None:
            print "Next Task Scheduled: %s" % self._NextTask
            nextTask = self.taskTree.findTask(self._NextTask)
            if nextTask == None:
                msg = "Error: Task named %s not found:\n" % self._NextTask
                msg += "Unable to continue processing\n"
                raise ShREEKException(msg, ClassInstance = self)
            self.executeTask(nextTask)
            return

        if self.monitorThread != None:
            self.monitorThread.notifyTaskStart(task)
        exitCode = taskRunner.run()
        if self.monitorThread != None:
            self.monitorThread.notifyTaskEnd(task, exitCode)
        print "Task Execution Complete: %s Exit: %s" % (
            task.taskname(), exitCode,
            )
        

        taskRunner.evalEndControlPoint(self)
        if self._NextTask != None:
            print "Next Task Scheduled: %s" % self._NextTask,
            nextTask = self.taskTree.findTask(self._NextTask)
            if nextTask == None:
                msg = "Error: Task named %s not found:\n" % self._NextTask
                msg += "Unable to continue processing\n"
                raise ShREEKException(msg, ClassInstance = self)

            self.executeTask(nextTask)
            return

        for child in task.children:
            self.executeTask(child)
        return
    
        
            
        
            
            
