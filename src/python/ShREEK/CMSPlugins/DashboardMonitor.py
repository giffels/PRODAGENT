#!/usr/bin/env python
"""
_DashboardMonitor_

MonALISA ApMon based monitoring plugin for ShREEK to broadcast data to the
CMS Dashboard

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: DashboardMonitor.py,v 1.1 2006/04/10 17:38:43 evansde Exp $"
__author__ = "evansde@fnal.gov"



from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo

import os
import time

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
        self.dashboardInfo['JobStarted'] = time.time()
        self.dashboardInfo.publish(5)

    #  //
    # // Task started
    #//
    def taskStart(self, task):
        """
        Tasked started notifier. 
        """
        if self.dashboardInfo == None:
            return
        self.dashboardInfo['ExeStart'] = task.taskname()
        self.dashboardInfo['ExeStartTime'] = time.time()
        self.dashboardInfo.publish(5)
        return
    
    def taskEnd(self, task, exitCode):
        """
        Tasked ended notifier.
        """
        if self.dashboardInfo == None:
            return
        
        
        self.dashboardInfo['ExeEnd'] = task.taskname()
        self.dashboardInfo['ExeFinishTime'] = time.time()
        self.dashboardInfo['ExeExitCode'] = exitCode
        self.dashboardInfo.publish(5)
        return

    def jobEnd(self):
        """
        Job ended notifier.
        """
        if self.dashboardInfo == None:
            return
        self.dashboardInfo['JobFinished'] = time.time()
        self.dashboardInfo.publish(5)
        
        
registerShREEKMonitor(DashboardMonitor, 'dashboard')
