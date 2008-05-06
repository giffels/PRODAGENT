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


from ProdCommon.MCPayloads.JobSpec import JobSpec

def loadJobSpec():
    """
    _loadJobSpec_

    Load the JobSpec

    """
    try:
        jobSpecFile = os.path.expandvars(os.environ["PRODAGENT_JOBSPEC"])
        jobSpec = JobSpec()
        jobSpec.load(jobSpecFile)
    except Exception, ex:
        print "ERROR: Cannot load JobSpec file!!!"
        jobSpec = None
    return jobSpec


class BulkEventMonitor(ShREEKMonitor):
    """
    _BulkEventMonitor_

    ShREEK Monitor that broadcasts data to MonALISA about live event counts
    from bulk created jobs
    
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

        jobSpec = loadJobSpec()
        paName = jobSpec.parameters['ProdAgentName']
        jobSpecId = jobSpec.parameters['JobName']

        cluster = paName
        node = jobSpecId
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
        self._CheckEvents()

        if self.eventLogger == None:
            return
        
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
        for i in range(0, 1):
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
        for i in range(0, 1):
            self.apmon.send(TaskStarted = timenow,
                            TaskName = str(task.taskname()))
            
                            
            
        self.apmon.disconnect() 
            
        return
        
    
    def taskEnd(self, task, exitCode):
        """
        Tasked ended notifier.
        """
        self._LoadEventLogger()
        self._CheckEvents()

        
        lastRun = 0
        lastEvent = 0
        if self.eventLogger != None:
            lastRun = self.eventLogger.latestRun
            lastEvent = self.eventLogger.latestEvent
            
        self.eventLogger = None
        self.apmon.connect()
        timenow = int(time.time())
        for i in range(0, 1):
            self.apmon.send(
                RunNumber = lastRun,
                EventNumber = lastEvent,
                TaskFinished = timenow,
                TaskName = str(task.taskname()),
                ExitStatus = exitCode)
            
        self.apmon.disconnect()


    def jobEnd(self):
        """
        Job ended notifier.
        """
        self.apmon.connect()
        timenow = int(time.time())
        for i in range(0, 1):
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
        if not os.path.exists(self.eventFile):
            self.eventLogger = None
        else:
            self.eventLogger = EventLogger(self.eventFile)
        return

    def _CheckEvents(self):
        """
        _CheckEvents_

        Get last available run/event number

        """
        if self.eventLogger == None:
            return

        try:
            self.eventLogger()
        except Exception:
            print "Error Calling Event Logger:", ex
            pass
        return 


registerShREEKMonitor(BulkEventMonitor, 'bulk-event')
