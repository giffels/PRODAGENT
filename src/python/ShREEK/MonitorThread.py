"""
Monitor thread module.
"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: MonitorThread.py,v 1.1 2005/12/30 18:54:25 evansde Exp $"

import threading
from ShREEK.ShREEKMonitorMgr import ShREEKMonitorMgr

from ShLogger.LogInterface import LogInterface
from ShLogger.LogStates import LogStates

class MonitorThread(threading.Thread, LogInterface):
    """
    _MonitorThread_

    Thread based monitor handling class, dispatches
    monitoring callbacks from the ExecutionThread to the
    Monitors via an instance of the Monitor Manager Object.
    """
    def __init__(self, exeMgr):
        threading.Thread.__init__(self)
        LogInterface.__init__(self)
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
        self.log("Set Interval to %s" % interval, LogStates.Info)
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
        self.log("MonitorState: Shutdown called", LogStates.Info)
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
        self.log("MonitorThread: JobStarted", LogStates.Info)
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
        self.log("MonitorThread: Task Started: %s" % task, LogStates.Info)
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
        self.log("Task Ended: %s with Exit Code:%s" % (task, exitCode)
                 , LogStates.Info)
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
        self.log("MonitorThread: JobEnded", LogStates.Info)
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
        self.log("MonitorThread: JobKilled", LogStates.Info)
        self._MonMgr.jobKilled()
        self.shutdown()
    #  //
    # //  Task has been killed
    #//
    def notifyKillTask(self):
        """
        Task has been killed.
        """
        self.log("MonitorThread: TaskKilled", LogStates.Info)
        self._MonMgr.taskKilled()
        
    #  //
    # // Override Thread.run() to do the periodic update
    #//  of the MonitorState object and dispatch it to the monitors
    def run(self):
        """
        Override Thread.run() to do the periodic update
        of the MonitorState object and dispatch it to the monitors
        """
        self.log("run", LogStates.Dbg_lo)
        while 1:
            #  //
            # // shutdown signal
            #//
            if self._Finished.isSet():
                self.log("run:_Finished.isSet()=1", LogStates.Dbg_hi)
                return

            #  //
            # // Update State information only during a running task
            #//
            if self._RunUpdate.isSet():
                self.log("run:_RunUpdate.isSet()=1", LogStates.Dbg_hi)
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
    
