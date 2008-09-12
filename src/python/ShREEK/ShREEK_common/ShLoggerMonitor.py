#!/usr/bin/env python
"""
Logger monitor module.
"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ShLoggerMonitor.py,v 1.1 2005/12/30 18:54:28 evansde Exp $"

from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

from ShLogger.LogInterface import LogInterface
from ShLogger.LogStates    import LogStates

class ShLoggerMonitor(ShREEKMonitor, LogInterface):
    """
    Logger monitor class.
    """
    def __init__(self):
        """
        Constructor.
        """
        ShREEKMonitor.__init__(self)
        LogInterface.__init__(self)

        
 
    def initMonitor(self, *args, **kwargs):
        """
        Initialize monitor method.
        """
        self.log("ShLoggerMonitor.InitMonitor", LogStates.Info)

    def shutdown(self):
        """
        Shutdown method.
        """
        self.log("ShLoggerMonitor.Shutdown", LogStates.Info)
        
    def periodicUpdate(self, monitorState):
        """
        Periodic update method.
        """
        self.log("ShLoggerMonitor.PeriodicUpdate", LogStates.Info)
        


registerShREEKMonitor(ShLoggerMonitor, "shlogger")
