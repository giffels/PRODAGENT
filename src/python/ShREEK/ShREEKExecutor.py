#!/usr/bin/env python
"""
_ShREEKExecutor_

Interface class for running the ShREEK system, including signal handlers
for safe execution

ShREEKExecutor provides a naked ShREEK System that can be configured
directly, ShREEKConfigExecutor wraps this so that a ShREEKConfig object
can be used to configure the ShREEK system.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ShREEKExecutor.py,v 1.1 2006/04/10 17:38:42 evansde Exp $"
__author__ = "evansde@fnal.gov"

import signal
from ShREEK.ExecutionManager import ExecutionManager
from ShREEK.MonitorThread import MonitorThread
from ShREEK.ShREEKException import ShREEKException


class ShREEKExecutor:
    """
    _ShREEKExecutor_

    ShREEK Execution interface to configure and execute ShREEK
    using a set of ShREEKTasks and monitoring configurations

    """
    def __init__(self):
        for signum in (1, 3, 6, 15):
            signal.signal(signum, self.safeShutdown)
        self.executionMgr = ExecutionManager()
        self.monitorThread = MonitorThread(self.executionMgr)
        self.pluginModules = []
        self.monitorConfigs = []
        self.updatorConfigs = []
        self.taskTree = None

        #  //
        # // control parameters
        #//
        self.verbose = False
        self.debug = False
        self.doMonitoring = True
        self.monitorInterval = 20
        self.jobID = None
        self.exitCode = 0

    def safeShutdown(self, signalNumber, stackFrame):
        """
        _safeShutdown_

        Signal handler for running a ShREEKTask set that
        reliably propagates signals to child processes

        """
        
        self.executionMgr.killjob()
        return
    

    
    def run(self):
        """
        _run_

        Run the ShREEK system

        """
        self.initialise()
        

        try:
            self.executionMgr.run()
            self.exitCode = self.executionMgr.exitCode()
        finally:
            self.monitorThread.shutdown()

        return
    
        
    def initialise(self):
        """
        _initialise_

        Load plugins and initialise the monitoring system with the
        flags provided in this instance

        """
        for plugin in self.pluginModules:
            self.loadShREEKPlugin(plugin)
            
        if self.taskTree == None:
            msg = "Task Tree is not provided, you must set the\n"
            msg += "ShREEKExecutor.taskTree attribute to provide a set\n"
            msg += "Of ShREEKTasks to run"
            raise ShREEKException(msg, ClassInstance = self)
        self.executionMgr.taskTree = self.taskTree
        self.executionMgr.jobID = self.jobID
        self.monitorThread.setInterval(self.monitorInterval)
        self.monitorThread.initMonitorFwk(self.monitorConfigs,
                                          self.updatorConfigs)
        if not self.doMonitoring:
            self.monitorThread.disableMonitoring()
        self.executionMgr.monitorThread = self.monitorThread
        return
    
        
        
    def loadShREEKPlugin(self, moduleName):
        """
        _loadSHREEKPlugin_
        
        Import a plugin module name that is on the pythonpath
        
        """
        
        try:
            __import__(moduleName, globals(), locals() , [''])
        except ImportError, ex:
            msg = "WARNING: Loading of ShREEK Plugin module failed:\n"
            msg += "Failed to import %s\n" % moduleName
            msg += str(ex)
            raise ShREEKException(msg, MissingModule = moduleName)
        return
    


class ShREEKConfigExecutor:
    """
    _ShREEKConfigExecutor_

    ShREEKExecutor wrapper that uses a ShREEKConfig instance
    to configure and run the ShREEK framework, as dictated by the
    ShREEKConfig object provided

    """
    def __init__(self, shreekConfigInstance):
        self.config = shreekConfigInstance
        self.executor = ShREEKExecutor()

        for plugin in self.config.listPluginModules():
            self.executor.pluginModules.append(plugin)

        for monitor in self.config.listMonitorCfgs():
            self.executor.monitorConfigs.append(monitor)

        for updator in self.config.listUpdators():
            self.executor.updatorConfigs.append(updator)

        self.executor.taskTree = self.config.taskTree()
        self.executor.jobID = self.config.jobId()
        
        #  //
        # // control parameters
        #//
        self.verbose = False
        self.debug = False
        self.doMonitoring = True
        self.monitorInterval = 20
        self.exitCode = 0
        
    def run(self):
        """
        run the ShREEK Framework

        """
        self.executor.verbose = self.verbose
        self.executor.debug = self.debug
        self.executor.doMonitoring = self.doMonitoring
        self.executor.monitorInterval = self.monitorInterval
        self.executor.run()
        
        self.exitCode = self.executor.exitCode
