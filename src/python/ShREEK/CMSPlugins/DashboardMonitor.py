#!/usr/bin/env python
"""
_DashboardMonitor_

MonALISA ApMon based monitoring plugin for ShREEK to broadcast data to the
CMS Dashboard

"""
__version__ = "$Revision: 1.4 $"
__revision__ = "$Id: DashboardMonitor.py,v 1.4 2006/06/27 21:19:42 evansde Exp $"
__author__ = "evansde@fnal.gov"



from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo

import os
import time
import socket

_GridJobIDPriority = [
    'EDG_WL_JOBID',
    'GLITE_WMS_JOBID',
    'GLOBUS_GRAM_JOB_CONTACT',
    ]


class DashboardMonitor(ShREEKMonitor):
    """
    _DashboardMonitor_

    ShREEK Monitor that broadcasts data to the CMS Dashboard using ApMon

    """
    
    def __init__(self):
        ShREEKMonitor.__init__(self)
        self.destPort = None
        self.destHost = None
        self.dashboardInfo = None
        self.lastExitCode = None
        

    def initMonitor(self, *args, **kwargs):
        """
        _initMonitor_

        """
        self.destHost = kwargs['ServerHost']
        self.destPort = int(kwargs['ServerPort'])
        dashboardInfoFile = kwargs['DashboardInfo']
        dashboardInfoFile = os.path.expandvars(dashboardInfoFile)
        
        self.dashboardInfo = DashboardInfo()
        try:
            self.dashboardInfo.read(dashboardInfoFile)
            self.dashboardInfo.addDestination(self.destHost, self.destPort)
        except StandardError, ex:
            msg = "ERROR: Unable to load Dashboard Info File:\n"
            msg += "%s\n" % dashboardInfoFile
            msg += "Unable to communicate to Dashboard\n"
            print msg
            self.dashboardInfo = None
        

    def shutdown(self):
        """
        Shutdown method, will be called before object is deleted
        at end of job.
        """  
        del self.dashboardInfo
        
    def jobStart(self):
        """
        Job start notifier.
        """
        if self.dashboardInfo == None:
            return

       
        gridJobId = None
        for envVar in _GridJobIDPriority:
            val = os.environ.get(envVar, None)
            if val != None:
                gridJobId = val
                break
        print "Dashboard Grid Job ID: %s" % gridJobId
        self.dashboardInfo['GridJobID'] = gridJobId
        self.dashboardInfo['JobStarted'] = time.time()
        self.dashboardInfo['SyncCE'] = socket.gethostname()
        self.dashboardInfo.publish(5)
        return

    #  //
    # // Task started
    #//
    def taskStart(self, task):
        """
        Tasked started notifier. 
        """
        if self.dashboardInfo == None:
            return

        newInfo = self.dashboardInfo.emptyClone()
        newInfo['ExeStart'] = task.taskname()
        newInfo['ExeStartTime'] = time.time()
        newInfo.publish(5)
        return
    
    def taskEnd(self, task, exitCode):
        """
        Tasked ended notifier.
        """
        if self.dashboardInfo == None:
            return

        exitFile = os.path.join(task.directory(), "exit.status")
        exitValue = exitCode
        if os.path.exists(exitFile):
            content = file(exitFile).read().strip()
            try:
                exitValue = int(content)
            except ValueError:
                exitValue = exitCode
                
                
        self.lastExitCode = exitValue
        newInfo = self.dashboardInfo.emptyClone()
        newInfo['ExeEnd'] = task.taskname()
        newInfo['ExeFinishTime'] = time.time()
        newInfo['ExeExitStatus'] = exitValue
        newInfo.publish(5)
        return

    def jobEnd(self):
        """
        Job ended notifier.
        """
        if self.dashboardInfo == None:
            return
        newInfo = self.dashboardInfo.emptyClone()
        newInfo['JobExitStatus'] = self.lastExitCode
        newInfo['JobFinished'] = time.time()
        newInfo.publish(5)
        
        
registerShREEKMonitor(DashboardMonitor, 'dashboard')
