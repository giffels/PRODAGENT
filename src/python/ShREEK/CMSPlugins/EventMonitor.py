#!/usr/bin/env python
"""
_EventMonitor_

Provide a running run/event update to a MonALISA ApMon server

"""

import time
import os

from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor
from ShREEK.CMSPlugins.ApMonLite.ApMonDestMgr import ApMonDestMgr
from ShREEK.CMSPlugins.EventLogger import EventLogger

from IMProv.IMProvQuery import IMProvQuery

class EventMonitor(ShREEKMonitor):
    """
    _DashboardMonitor_

    ShREEK Monitor that broadcasts data to the CMS Dashboard using ApMon

    """
    
    def __init__(self):
        ShREEKMonitor.__init__(self)
        self.apmon = None
        self.eventFile = None
        self.eventLogger = None
        self.currentTask = None
        
    def initMonitor(self, *args, **kwargs):
        """
        _initMonitor_

        """
        cluster = kwargs['ProdAgentID']
        node = kwargs['ProdAgentJobID']
        self.apmon = ApMonDestMgr(cluster, node)

        self.eventFile = kwargs['EventFile']

        destQ = IMProvQuery("ShREEKMonitorCfg/Destination")
        dests = destQ(self.monitorConfig)

        for dest in dests:
            destPort = int(dest.attrs['Port'])
            destHost = str(dest.attrs['Host'])
            self.apmon.newDestination(destHost, destPort)
            

    def periodicUpdate(self, monitorState):
        """
        Periodic update.
        """
        self._LoadEventLogger()
        print ">>>>>>>>>>>>>>>>>>>>>>>>>>>> periodic update"
        if self.eventLogger == None:
            print "No Event Logger"
            return

        try:
            self.eventLogger()
        except Exception:
            print "Error Calling Event Logger:", ex
            pass
        
        print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>"
        print self.eventLogger.latestRun, self.eventLogger.latestEvent
        
        print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>"
        self.apmon.connect()
        timenow = int(time.time())
        self.apmon.send(EventUpdate = timenow,
                        RunNumber = self.eventLogger.latestRun,
                        EventNumber = self.eventLogger.latestEvent
                        )
        self.apmon.disconnect()
        return

    #  //
    # // Start of job notifier
    #//
    def jobStart(self):
        """
        Job start notifier.
        """
        self.apmon.connect()
        timenow = int(time.time())
        for i in range(0, 5):
            self.apmon.send(JobStarted = timenow)
        
        self.apmon.disconnect()
        return

    #  //
    # // Task started
    #//
    def taskStart(self, task):
        """
        Tasked started notifier. 
        """
        self.currentTask = task
        self.apmon.connect()
        timenow = int(time.time())
        for i in range(0, 5):
            self.apmon.send(TaskStarted = timenow,
                            TaskName = str(task.taskname()))
            
                            
            
        self.apmon.disconnect() 
            
        return
        
    
    def taskEnd(self, task, exitCode):
        """
        Tasked ended notifier.
        """
        self.eventLogger = None
        self.apmon.connect()
        timenow = int(time.time())
        for i in range(0, 5):
            self.apmon.send(TaskFinished = timenow,
                            TaskName = str(task.taskname()),
                            ExitStatus = exitCode)
            
        self.apmon.disconnect()


    def jobEnd(self):
        """
        Job ended notifier.
        """
        self.apmon.connect()
        timenow = int(time.time())
        for i in range(0, 5):
            self.apmon.send(JobFinished = timenow)
        self.apmon.disconnect()
        return

    def _LoadEventLogger(self):
        """
        _LoadEventLogger_

        Look for file and load it if it exists

        """
        if self.eventLogger != None:
            return
        filePath = os.path.join(self.currentTask.directory(), self.eventFile)
        if not os.path.exists(filePath):
            self.eventLogger = None
        else:
            self.eventLogger = EventLogger(filename)
        return

registerShREEKMonitor(EventMonitor, 'event')
