#pylint: disable-msg=W0152
#
# disable pylint bad ** magic warning
#
"""
ShREEK monitor manager module.
"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ShREEKMonitorMgr.py,v 1.1 2005/12/30 18:54:25 evansde Exp $"

from ShREEK.ShREEKException import ShREEKException
from ShREEK.ShREEKPluginMgr import ShREEKPlugins
from ShREEK.MonitorState import MonitorState

from ShLogger.LogStates import LogStates
from ShLogger.LogInterface import LogInterface


class ShREEKMonitorMgr(dict, LogInterface):
    """
    Monitor Manager object for distributing calls to the Monitor
    Objects and managing them in sensible way.
    """

    def __init__(self, executionMgrRef):
        """
        Constructor taking executor reference, and configuration
        instance.
        """
        dict.__init__(self)
        LogInterface.__init__(self)
        self.executionMgr = executionMgrRef
        self.monitorConfig = None
        self.updatorConfig = None
        self.ignoreErrors = False
        self.state = MonitorState()
        

    def ignoreErrorsInMonitors(self):
        """
        _ignoreErrorsInMonitors_

        Do not propagate execptions from the monitoring system to the
        execution system.

        """
        self.ignoreErrors = True
        self.state.ignoreErrors = True
        return
        
    def loadMonitors(self):
        """
        _loadMonitors_

        Call out to the plugin loader to load the 
        load the monitors.
        """
        if self.monitorConfig == None:
            return
        for item in self.monitorConfig:
            #  //
            # // First try and load the monitor object
            #//
            try:
                monName = item.monitorName()
                monitor = self._LoadMonitorInstance(item.monitorType())
                monitor.monitorConfig = item
                monitor.executionMgr = self.executionMgr
                monitor.jobId = self.executionMgr.jobID
                self[monName] = monitor
            except StandardError, ex:
                if self.ignoreErrors:
                    msg = "WARNING: Error ignored in Monitoring System:\n"
                    msg += "Caught Instantiating Monitor:\n"
                    msg += "Name %s Type: %s\n" % (monName, item.monitorType())
                    msg += str(ex)
                    self.log(msg, LogStates.Alert)
                    continue
                else:
                    raise ex
                
            #  //
            # // Now try and init the object if it loaded OK
            #//
            try:
                self[monName].initMonitor(*item.positionalArgs(),
                                          **item.keywordArgs())
                
            except StandardError, ex:
                if self.ignoreErrors:
                    msg = "WARNING: Error ignored in Monitoring System:\n"
                    msg += "Caught Initialising Monitor:\n"
                    msg += "Name %s Type: %s\n" % (monName, item.monitorType())
                    msg += str(ex)
                    self.log(msg, LogStates.Alert)
                    continue
                else:
                    raise ex


    def _LoadMonitorInstance(self, monitorType):
        """
        _LoadMonitorInstance_

        Create and return a new instance of the Monitor Object of the
        type requested from the Plugin system

        """
        try:
            newMonitor = ShREEKPlugins.getMonitor(monitorType)
        except ShREEKException, ex:
            self.log(
                "Load Monitor Failed: %s" % monitorType, 
                LogStates.Alert)
            raise ex
        except Exception, ex:
            msg = "Exception while instantiating Monitor of Type:\n"
            msg += monitorType
            msg += "\nException Details:\n"
            msg += str(ex)
            self.log(msg, LogStates.Alert)
            raise ShREEKException(msg, MonitorType = monitorType,
                                  ClassInstance = self)

        if newMonitor == None:
            msg = "WARNING: Failed to load Monitor of Type:\n"
            msg += "%s\n" % monitorType
            msg += "Monitor Type may not be registered with the Plugin Manager"
            self.log(msg, LogStates.Alert)
            raise ShREEKException(msg, MonitorType = monitorType,
                                  ClassInstance = self)
        return newMonitor
    


        


    def loadUpdators(self):
        """
        _loadUpdators_
        
        Load the plugins for the MonitorState
        """
        if self.updatorConfig == None:
            return
        
        updators = self.updatorConfig
        toLoad = {}
        for updtr in updators:
            updtrName = updtr.updatorName()
            try:
                updRef = self._LoadUpdatorMethod(updtrName)
                toLoad[updtrName] = updRef
            except ShREEKException, ex:
                if self.ignoreErrors:
                    msg = "WARNING: Error ignored in Monitoring System:\n"
                    msg += "Caught Loading Updator:\n"
                    msg += "Updator Name %s\n" % updtrName
                    msg += str(ex)
                    self.log(msg, LogStates.Alert)
                    continue
                else:
                    raise ex
                

        self.state.loadExtenFields(toLoad)
        return

    def _LoadUpdatorMethod(self, updatorName):
        """
        _LoadUpdatorMethod_

        Load an updator reference from the plugin manager using the name
        provided.

        """
        try:
            newUpdator = ShREEKPlugins.getUpdator(updatorName)
        except ShREEKException, ex:
            self.log(
                "LoadUpdatorMethod: Load Failed: %s" % updatorName, 
                LogStates.Alert)
            raise ex
        if newUpdator == None:
            msg = "WARNING: Failed to load Updator:\n"
            msg += "%s\n" % updatorName
            msg += "Updator may not be registered with the Plugin Manager"
            self.log(msg, LogStates.Alert)
            raise ShREEKException(msg, UpdatorName = updatorName,
                                  ClassInstance = self)
        return newUpdator
    
    


    def shutdown(self):
        """
        _shutdown_

        Shutdown call to all monitors
        """
        try:
            for k in self.keys():
                self[k].shutdown()
        except StandardError, ex:
            msg = "Error in Shutdown from Monitor: %s\n" % k
            msg += "Details:\n%s" % str(ex)
            self.log(msg, LogStates.Alert)
        for k in self.keys():
            del self[k]
        return

    def periodicUpdate(self):
        """
        _periodicUpdate_

        Call the PeriodicUpdate for all the Monitors, passing the
        MonitorThread MonitorState instance with the current information

        Args --
        
        """
        self.state['CurrentProcess'] = self.executionMgr.currentTask.process
        try:
            self.state.updateState()
        except StandardError, ex:
            if self.ignoreErrors:
                msg = "WARNING: Error ignored in Monitoring System:\n"
                msg += "Caught Updating MonitorState:\n"
                msg += str(ex)
                self.log(msg, LogStates.Alert)
            else:
                raise ex
        
        for key in self.keys():
            try:
                self[key].periodicUpdate(self.state)
            except StandardError, ex:
                if self.ignoreErrors:
                    msg = "WARNING: Error ignored in Monitoring System:\n"
                    msg += "Caught on PeriodicUpdate from %s\n" % key
                    msg += str(ex)
                    self.log(msg, LogStates.Alert)
                    continue
                else:
                    raise ex
        return

    def jobStart(self):
        """
        Call job start for each monitor.
        """
        for key in self.keys():
            self[key].jobStart()
        return
        
    def taskStart(self, task):
        """
        Call task start for each monitor.
        """
        for key in self.keys():
            self[key].taskStart(task)
        return

    def taskEnd(self, task, exitCode):
        """
        Call task end for each monitor.
        """
        for key in self.keys():
            self[key].taskEnd(task, exitCode)
        return

    def jobEnd(self):
        """
        Call job end for each monitor.
        """
        for key in self.keys():
            self[key].jobEnd()
        return

    def jobKilled(self):
        """
        Call job killed for each monitor.
        """
        for key in self.keys():
            self[key].jobKilled()
        return

    def taskKilled(self):
        """
        Call task killed for each monitor.
        """
        for key in self.keys():
            self[key].taskKilled()
        return

