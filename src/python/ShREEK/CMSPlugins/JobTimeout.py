#!/usr/bin/env python
"""
_JobTimeout_

If the total elapsed (real) time of the job exceeds a predefined
threshold provided in the monitor configuration, the job will be shutdown
at that time.

"""


from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

from ShREEK.CMSPlugins.ApMon.DashboardAPI import DashboardAPI

import os
import time

class JobTimeout(ShREEKMonitor):
    """
    _JobTimeout_

    ShREEK Monitor that measures the time of the job against a cut off
    and shutsdown the job should it exceed that time limit
    
    """
    
    def __init__(self):
        ShREEKMonitor.__init__(self)
        self.timeoutValue = None
        self.startTime = None
        self.checkTimeout = False
        
    def initMonitor(self, *args, **kwargs):
        """
        _initMonitor_

        """
        if kwargs.has_key("Timeout"):
            self.timeoutValue = int(kwargs['Timeout'])
            self.startTime = int(time.time())
            self.checkTimeout = True


    def periodicUpdate(self, state):
        """
        _periodicUpdate_

        check time every periodic update cycle

        """
        if self.checkTimeout:
            timeDiff = int(time.time()) - self.startTime
            if timeDiff > self.timeoutValue:
                msg = ""
                for lines in range(0,4):
                    for columns in range(0, 51):
                        msg += "#"
                    msg += "\n"
                msg += "WARNING: Job Timeout has Expired:"
                msg += "Start Time: %s\n" % self.startTime
                msg += "Time Now: %s\n" % time.time()
                msg += "Timeout: %s\n" % self.timeoutValue
                msg += "Killing Job...\n"
                for lines in range(0,4):
                    for columns in range(0, 51):
                        msg += "#"
                    msg += "\n"
                    
                
                self.killJob()
        return
        
        

        
registerShREEKMonitor(JobTimeout, 'timeout')



