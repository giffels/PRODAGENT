#!/usr/bin/env python
"""
_ShREEKInterface_

Inheritable or instantiable interface object to
contain and manipulate a ShREEKConfig object
and all the pieces therin

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ShREEKInterface.py,v 1.1 2005/12/30 18:54:25 evansde Exp $"

import os
from ShREEK.ShREEKConfig import ShREEKConfig
from ShREEK.ShREEKTask import ShREEKTask
from ShREEK.ShREEKMonitorCfg import ShREEKMonitorCfg
from ShREEK.ShREEKException import ShREEKException



class ShREEKInterface:
    """
    _ShREEKInterface_
    
    Interface class for building and manipulating
    a ShREEK Config object
    """
    def __init__(self):
        self._ShREEKConfig = ShREEKConfig()
        self._ShREEKMonitors = {}


    def setJobId(self, jobid):
        """
        _setJobId_

        Set the global ID for this job
        """
        self._ShREEKConfig.setJobId(jobid)

    def jobId(self):
        """
        _jobId_

        return the current job id
        """
        returnself._ShREEKConfig.jobId()

    def writeShREEKConfig(self, filename):
        """
        _writeShREEKConfig_

        Write out a ShREEK Config file using the
        path/filename provided.
        """
        self._ShREEKConfig.save(filename)
        return


    def listShREEKPlugins(self):
        """return a list of current plugin modules"""
        return self._ShREEKConfig.listPluginModules()


    def addPluginModule(self, moduleName):
        """
        _addPluginModule_

        Add the name of a Module containing
        ShREEK Plugins to teh list of Plugin modules.
        NOTE: This module must be available on the python
        path AT RUNTIME.
        """
        
        if moduleName not in self.listShREEKPlugins():
            self._ShREEKConfig.addPluginModule(
                moduleName
                )
        return
    
    
    def addUpdator(self, updatorName):
        """
        _addUpdator_

        Add an Updator plugin to the list of
        plugins to be used.

        """
        
        if updatorName not in self._ShREEKConfig.listUpdators():
            self._ShREEKConfig.addUpdator(
                updatorName
                )
        return
    

    def addMonitor(self, shreekMonitorCfg):
        """
        _addMonitor_

        Add a monitor configuration directly to the ShREEKConfig.
        The Argument must be an instance of ShREEKMonitorCfg
        
        """
        if not isinstance(shreekMonitorCfg, ShREEKMonitorCfg):
            msg = "Tried to add non ShREEKMonitorCfg instance\n"
            msg += "to ShREEKInterface.\n"
            msg += "The argument must be an instance of ShREEKMonitorCfg\n"
            raise ShREEKException(msg, ClassInstance = self,
                                  BadObject = shreekMonitorCfg)

        self._ShREEKConfig.addMonitorCfg(shreekMonitorCfg)
        self._ShREEKMonitors[shreekMonitorCfg.monitorName()] = shreekMonitorCfg
        return
        
    def newMonitor(self, monitorName, monitorType):
        """
        _newMonitor_

        Create a new Monitor Configuration object in
        the ShREEKConfig with the name and type provided

        Args --

        - *monitorName* : Identifying name for the monitor
        instance
              
        - *monitorType* : The name of the monitor plugin
        to be instantiated, eg stdout, shlogger etc
              
        """
        if monitorName in self._ShREEKMonitors.keys():
            msg = "Tried to add Duplicate monitor:\n"
            msg += "%s\n" % monitorName
            msg += "To ShREEKInterface, existsing names:\n"
            msg += str(self._ShREEKMonitors.keys())
            raise ShREEKException(
                msg, ClassInstance = self,
                DuplicateName = monitorName,
                ExistingNames = self._ShREEKMonitors.keys())
        
        newMonitor = ShREEKMonitorCfg(MonitorName = monitorName,
                                      MonitorType = monitorType)
        
        self._ShREEKMonitors[monitorName] = newMonitor
        self._ShREEKConfig.addMonitorCfg(newMonitor)
        return

    def configureMonitor(self, monName, *posArgs, **kwargs):
        """
        _configureMonitor_

        configure the monitor Configuration for the
        name provided with the positional and keyword
        args provided.
        """
        monitorRef =  self._ShREEKMonitors.get(monName, None)
        if monitorRef == None:
            msg = "Tried to configure Non-existent monitor:"
            msg += "\n%s\n" % monName
            msg += "Existing Monitors:\n"
            msg += str(self._ShREEKMonitors.keys())
            raise ShREEKException(
                msg, ClassInstance = self,
                MissingMonitor = monName,
                ValidMonitors = self._ShREEKMonitors.keys())

        monitorRef.addPositionalArg(*posArgs)
        monitorRef.addKeywordArg(**kwargs)
        return
    
        

    def setMonitorParam(self, monName, *params):
        """
        _setMonitorParam_

        Add the parameter provided to the positional
        args of the monitor named monName
        """
        monitorRef =  self._ShREEKMonitors.get(monName, None)
        if monitorRef == None:
            msg = "Tried to configure Non-existent monitor:"
            msg += "\n%s\n" % monName
            msg += "Existing Monitors:\n"
            msg += str(self._ShREEKMonitors.keys())
            raise ShREEKException(
                msg, ClassInstance = self,
                MissingMonitor = monName,
                ValidMonitors = self._ShREEKMonitors.keys())
        monitorRef.addPositionalArg(*params)
        return

    def setMonitorOption(self, monName, **options):
        """
        _setMonitorOption_

        Configure the named monitor by adding the provided
        options to its keyword args
        """
        monitorRef =  self._ShREEKMonitors.get(monName, None)
        if monitorRef == None:
            msg = "Tried to configure Non-existent monitor:"
            msg += "\n%s\n" % monName
            msg += "Existing Monitors:\n"
            msg += str(self._ShREEKMonitors.keys())
            raise ShREEKException(
                msg, ClassInstance = self,
                MissingMonitor = monName,
                ValidMonitors = self._ShREEKMonitors.keys())
        monitorRef.addKeywordArg(**options)
        return
    

    
    def addTask(self, task):
        """
        _addTask_

        Add a new task to the task list with the name
        provided. If the tasklist is not specified, then
        the task is added to the first tasklist.
        """
        if isinstance(task, ShREEKTask):
            self._ShREEKConfig.addTask(task)
            return 
        if type(task) == type("string"):
            dirname = os.path.dirname(task)
            exename = os.path.basename(task)
            taskObject = ShREEKTask(Directory = dirname,
                                    Executable = exename)
            
            self._ShREEKConfig.addTask(taskObject)
            return 
        
        msg = "Unknown Task type added to ShREEKInterface\n"
        msg += "\t%s\n" % task
        msg += "Argument must be a ShREEKTask Object or a path to\n"
        msg += "an executable script\n"
        raise ShREEKException(msg, ClassInstance = self,
                              BadObject = task)
    



        
