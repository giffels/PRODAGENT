#!/usr/bin/env python
# pylint: disable-msg=W0613

"""
Standard output logger monitor module.
"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: StdoutMonitor.py,v 1.1 2005/12/30 18:54:28 evansde Exp $"

import os

from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor


class StdoutMonitor(ShREEKMonitor):
    """
    Standard output monitor class.
    """
    def __init__(self):
        """
        Constructor.
        """
        ShREEKMonitor.__init__(self)
        self._LogFile = None

    def initMonitor(self, *args, **kwargs):
        """
        Initialize monitor method
        """
        print "StdoutMonitor.InitMonitor"
        

    def shutdown(self):
        """
        Shutdown method.
        """
        print "StdoutMonitor.Shutdown"

    def periodicUpdate(self, monitorState):
        """
        Periodic update method.
        """
        #print "Current Process", self.currentProcessID()
        print "State: %s" % monitorState
        if self._LogFile == None:
            if os.path.exists("./task-stdout-stderr.log"):
                self._LogFile = open("./task-stdout-stderr.log", 'r')
        else:
            output = self._LogFile.read()
            print output
        

    #  //
    # // Start of job notifier
    #//
    def jobStart(self):
        """
        Job start notifier.
        """
        print "StdoutMonitor.JobStart"

    #  //
    # // Task started
    #//
    def taskStart(self, task):
        """
        Task start notifier for task provided.
        """
        print "StdoutMonitor.TaskStart:%s" % task.taskname()
        
    
    def taskEnd(self, task, exitCode):
        """
        Task end notifier for task provided with exit code.
        """
        if self._LogFile != None:
            output = self._LogFile.read()
            print output
            self._LogFile.close()
            self._LogFile = None
        print "StdoutMonitor.TaskEnd:%s with code %s" % (task.taskname(), exitCode)
        return
        

    def jobEnd(self):
        """
        Job end notifier.
        """
        print "StdoutMonitor.JobEnd"

    def jobKilled(self):
        """
        Job killed notifier.
        """
        print "StdoutMonitor. JobKilled"

    def taskKilled(self):
        """
        Task killed notifier.
        """
        print "StdoutMonitor.TaskKilled"

    def taskSuspend(self):
        """
        Task suspend notifier.
        """
        print "StdoutMonitor.TaskSuspend"

    def taskResume(self):
        """
        Task resume notifier.
        """
        print "StdoutMonitor.TaskResume"

    def taskRestart(self):
        """
        Tasl restart notifier.
        """
        print "StdoutMonitor.TaskRestart"

    def taskKillRestart(self):
        """
        Task kill and restart notifier.
        """
        print "StdoutMonitor.TaskKillRestart"
        

registerShREEKMonitor(StdoutMonitor, 'stdout')
