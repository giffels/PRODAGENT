#!/usr/bin/env python
"""
_PerfMonitor_

Performance monitor that generates a formatted text file for cmsRun
processes so that they can be sorted and plotted to make standard performance
quality plots

"""

import time
import os


from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

from ShREEK.CMSPlugins.TraceUtils import getCommandOutput


_PsCommand = \
"""
ps -eo pid,ppid,rss,vsize,pcpu,pmem,cmd -ww | grep  " cmsRun " | grep -v grep | grep -v myprofile | grep `cat process_id`"""


class PerfMonitor(ShREEKMonitor):
    """
    _PerfMonitor_

    Periodically run a ps command at a set interval to
    generate a performance statistics file for the
    running cmsRun task

    """
    def __init__(self):
        ShREEKMonitor.__init__(self)
        self.currentTask = None
        self.currentReport = None
        self.reportFile = None

        
    def initMonitor(self, *args, **kwargs):
        """
        _initMonitor_

        """
        self.reportFile = kwargs.get("ReportFile", "PerfReport.log")
        

    def taskStart(self, task):
        """
        Task started notifier.  Start generating report
        """
        self.currentTask = task
        self.currentReport = os.path.join(self.currentTask.directory(),
                                          self.reportFile)
        if os.path.exists(self.currentReport):
            os.remove(self.currentReport)

        handle = open(self.currentReport, 'w')
        handle.write("")
        handle.close()
        
        return

    def taskEnd(self, task, exitCode):
        """
        Task Ended notifier. Stop generating report

        """
        self.currentTask = None
        return


    def periodicUpdate(self, monitorState):
        """
        _periodicUpdate_

        Every time this method is called, add another line
        of stats to the report file
        
        """
        if self.reportFile == None:
            return
        command = _PsCommand
        command += " >> %s " % self.reportFile
        getCommandOutput(command)
        return
    
    
registerShREEKMonitor(PerfMonitor, 'perf-monitor')
