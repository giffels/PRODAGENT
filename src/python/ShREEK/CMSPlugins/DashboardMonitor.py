#!/usr/bin/env python
"""
_DashboardMonitor_

MonALISA ApMon based monitoring plugin for ShREEK to broadcast data to the
CMS Dashboard

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: DashboardMonitor.py,v 1.1 2006/03/14 22:51:59 evansde Exp $"
__author__ = "evansde@fnal.gov"



from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

from ShREEK.CMSPlugins.ApMon.DashboardAPI import DashboardAPI

import os
import time

class DashboardMonitor(ShREEKMonitor):
    """
    _DashboardMonitor_

    ShREEK Monitor that broadcasts data to the CMS Dashboard using ApMon

    """
    
    def __init__(self):
        ShREEKMonitor.__init__(self)
        self.dashboard = None
        self.jobName = None
        self.requestName = None

    def initMonitor(self, *args, **kwargs):
        """
        _initMonitor_

        """
        self.jobName = kwargs['JobName']
        self.requestName = kwargs['RequestName']

        gridJobId = None
        if os.environ.has_key("GLOBUS_GRAM_JOB_CONTACT"):
            gridJobId = os.environ['GLOBUS_GRAM_JOB_CONTACT']
        if os.environ.has_key("EDG_WL_JOBID"):
            gridJobId = os.environ['EDG_WL_JOBID']

        if gridJobId != None:
            self.jobName = "%s_%s" % (self.jobName, gridJobId)

        self.dashboard = DashboardAPI(self.requestName, self.jobName)

    def shutdown(self):
        """
        Shutdown method, will be called before object is deleted
        at end of job.
        """  
        del self.dashboard
        
    def jobStart(self):
        """
        Job start notifier.
        """
        self.dashboard.publish(JobStarted = time.time())
        

    #  //
    # // Task started
    #//
    def taskStart(self, task):
        """
        Tasked started notifier. 
        """
        self.dashboard.publish(CurrentExe = task.taskname(),
                               ExeStarted = time.time())
        
        
    
    def taskEnd(self, task, exitCode):
        """
        Tasked ended notifier.
        """
        self.dashboard.publish(ExeFinished = time.time(),
                               ExeStatus = exitCode)


    def jobEnd(self):
        """
        Job ended notifier.
        """
        self.dashboard.publish(JobFinished = time.time())

        
registerShREEKMonitor(DashboardMonitor, 'dashboard')
