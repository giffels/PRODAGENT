"""
Monitor thread module.
"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: MonitorThread.py,v 1.1 2006/04/10 17:38:42 evansde Exp $"

import threading
from ShREEK.ShREEKMonitorMgr import ShREEKMonitorMgr


class MonitorThread(threading.Thread):
    """
    _MonitorThread_

    Thread based monitor handling class, dispatches
    monitoring callbacks from the ExecutionThread to the
    Monitors via an instance of the Monitor Manager Object.
    """
    def __init__(self, exeMgr):
        threading.Thread.__init__(self)
        self.doMonitoring = True
        self._Finished  =  threading.Event()
        self._EndOfJob  =  threading.Event()
        self._NewTask   =  threading.Event()
        self._JobKilled =  threading.Event()
        self._RunUpdate =  threading.Event()
        self._Interval  =  20.0
        self._MonMgr = ShREEKMonitorMgr(exeMgr)

    def setInterval(self, interval):
        """
        Set the monitor interval
        """
        print "Set Interval to %s" % interval
        self._Interval = interval

    def disableMonitoring(self):
        """
        _disableMonitoring_

        Turn off active monitoring (periodicUpdate calls are disabled,
        event driven monitoring still occurs
        """
        self.doMonitoring = False
        
        

    def getMonitorState(self):
        """
        Return Monitor State Reference.
        """
        return self._MonMgr.state


    
    def shutdown(self):
        """
        Shutdown the monitor.
        """
        print "MonitorState: Shutdown called"
        self._MonMgr.shutdown()
        self._Finished.set()

    #  //=========notify Methods called by the ExecutionManager====
    # //
    #//  Start notification from the exe thread, this starts the
    #  //periodic updates of the monitor thread
    # //
    #//
    def notifyJobStart(self):
        """
        Start the job.
        """
        print "MonitorThread: JobStarted"
        self._MonMgr.jobStart()
        if self.doMonitoring:
            self.setDaemon(1)
            self.start()
        return
    #  //
    # // notify Monitors of new task start up
    #//
    def notifyTaskStart(self, task):
        """
        notify Monitors of new task start up.
        """
        self._RunUpdate.set()
        print "MonitorThread: Task Started: %s" % task
        self._MonMgr.taskStart(task)
        return

    #  //
    # // notify Monitors of task completion
    #//
    def notifyTaskEnd(self, task, exitCode):
        """
        notify Monitors of task completion.
        """
        self._RunUpdate.clear()
        print "Task Ended: %s with Exit Code:%s" % (task, exitCode)
        self._MonMgr.taskEnd(task, exitCode)
        return

    #  //
    # // notify monitors of Job Completion, stops the periodic
    #//  updating
    def notifyJobEnd(self):
        """
        notify monitors of Job Completion, stops the periodic
        updating.
        """
        print "MonitorThread: JobEnded"
        self._MonMgr.jobEnd()
        self.shutdown()
        return
    #  //
    # //  Interrupt Notifiers
    #//   Job has been killed
    def notifyKillJob(self):
        """
        Interrupt Notifiers, Job has been killed.
        """
        print "MonitorThread: JobKilled"
        self._MonMgr.jobKilled()
        self.shutdown()
    #  //
    # //  Task has been killed
    #//
    def notifyKillTask(self):
        """
        Task has been killed.
        """
        print "MonitorThread: TaskKilled"
        self._MonMgr.taskKilled()
        
    #  //
    # // Override Thread.run() to do the periodic update
    #//  of the MonitorState object and dispatch it to the monitors
    def run(self):
        """
        Override Thread.run() to do the periodic update
        of the MonitorState object and dispatch it to the monitors
        """
        while 1:
            #  //
            # // shutdown signal
            #//
            if self._Finished.isSet():
                return

            #  //
            # // Update State information only during a running task
            #//
            if self._RunUpdate.isSet():
                self._MonMgr.periodicUpdate()

            #time.sleep(self._Interval)
            self._Finished.wait(self._Interval)
           

    #  //
    # // Load Monitor Objects based on Cfg settings passed
    #//  from Executor
    def initMonitorFwk(self, monitorCfg, updatorCfg):
        """
        _initMonitorFwk_

        Initialise the MonitorMgr object when this method is
        called from the Execution thread
        Load Monitor Objects based on Cfg settings passed from Executor.
        """
        self._MonMgr.monitorConfig = monitorCfg
        self._MonMgr.updatorConfig = updatorCfg
        self._MonMgr.loadMonitors()
        self._MonMgr.loadUpdators()
        return
    
