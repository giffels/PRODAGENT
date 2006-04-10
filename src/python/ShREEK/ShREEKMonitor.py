"""
ShREEK monitor module.
"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ShREEKMonitor.py,v 1.1 2005/12/30 18:54:25 evansde Exp $"

#  //
# // Base Monitor Class for ShREEK Monitor Objects
#//
class ShREEKMonitor:
    """
    Base Monitor Class for ShREEK Monitor Objects.
    """
    #  //
    # // Ctor must take _no_ Arguments since monitor objects
    #//  are dynamically loaded via a factory interface
    def __init__(self):
        """
        Constructor.
        """
        self.executionMgr = None
        self.monitorConfig = None
        self.jobID = None

    #  //
    # // Init method that can accept positional and keyword args
    #//  this should be used to init the object
    def initMonitor(self, *args, **kwargs):
        """
        Init method that can accept positional and keyword args
        this should be used to init the object
        """
        pass

    #  //
    # // Shutdown method, will be called before object is deleted
    #//  at end of job
    def shutdown(self):
        """
        Shutdown method, will be called before object is deleted
        at end of job.
        """
        pass


    #  //=====================================================
    # //   Monitoring Hook methods, override as needed.
    #//
    def periodicUpdate(self, monitorState):
        """
        Periodic update.
        """
        pass


    #  //
    # // Start of job notifier
    #//
    def jobStart(self):
        """
        Job start notifier.
        """
        pass

    #  //
    # // Task started
    #//
    def taskStart(self, task):
        """
        Tasked started notifier. 
        """
        pass

    
    def taskEnd(self, task, exitCode):
        """
        Tasked ended notifier.
        """
        pass

    def jobEnd(self):
        """
        Job ended notifier.
        """
        pass

    def jobKilled(self):
        """
        Job killed notifier.
        """
        pass

    def taskKilled(self):
        """
        Task killed notifier.
        """
        pass



    #  //
    # //   End of hook methods
    #//======================================================

  

    
    #  //
    # // Utility methods for monitors to access information
    #//
    def currentProcessID(self):
        """
        _currentProcessID_

        Return the process that corresponds to the currently executing
        task. Will be -1 if no task is running.

        """
        currTask = self.executionMgr.currentTask
        if currTask == None:
            return -1
        return currTask.process

    def killJob(self):
        """
        _killJob_

        Kill the current process and shutdown ShREEK
        """
        self.executionMgr.killjob()
        return

    def killTask(self):
        """
        _killTask_

        Kill the current task but allow execution to continue to the next task
        """
        self.executionMgr.killtask()
        return

    def jobComplete(self):
        """
        _jobComplete_

        Set the flag that the job is complete, when the current task
        finishes.

        """
        self.executionMgr.jobComplete()
        return
    
        
