#!/usr/bin/env python
"""
_ShREEKConfig_

Container class for a ShREEKConfiguration that can be serialised to XML
for later execution

"""

__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: ShREEKConfig.py,v 1.2 2006/03/14 22:51:59 evansde Exp $"

import os
import socket
from xml.sax import SAXParseException

import ShREEK.ShREEK_common

from ShREEK.ShREEKException import ShREEKException
from ShREEK.ShREEKTask import ShREEKTask
from ShREEK.ShREEKMonitorCfg import ShREEKMonitorCfg
from ShREEK.ShREEKMonitorCfg import ShREEKUpdatorCfg

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile



def newMonitor():
    """
    _newMonitor_

    Create a new, empty ShREEKMonitorCfg instance and return it

    """
    return ShREEKMonitorCfg()


class ShREEKConfig(IMProvNode):
    """
    ShREEK Configuration Object, contains all info to initialise a
    ShREEK instance for running.
    """
    newMonitorCfg = staticmethod(newMonitor)
    
    def __init__(self):
        IMProvNode.__init__(self, self.__class__.__name__)
        self.addNode(IMProvNode("ShREEKPlugins"))
        self.addNode(IMProvNode("ShREEKMonitors"))
        self.addNode(IMProvNode("ShREEKUpdators"))
        self.addPluginModule("ShREEK.ShREEK_common")
        self.addPluginModule("ShREEK.ControlPoints.ActionImpl")
        self.addPluginModule("ShREEK.ControlPoints.CondImpl")
        self._TaskTree = None
        
    def setJobId(self, nameString):
        """
        _setJobID_

        Set the JobID attribute to be used to identify this ShREEK
        job set
        """
        self.attrs['JobId'] = str(nameString)
        
        
    def jobId(self):
        """
        _jobId_

        Retrieve the JobID attribute
        """
        jobName = self.attrs.get('JobId', "ShREEK-%s-%s" % (
            socket.gethostname(), os.getpid()))
        return jobName

        
            
    def pluginModules(self):
        """
        Return a reference to the Plugin Modules IMProvNode
        """
        return self["ShREEKPlugins"][0]

    def addPluginModule(self, moduleName):
        """
        _addPluginModule_
        """
        node = IMProvNode("ShREEKPlugin", Module = moduleName)
        self.pluginModules().addNode(node)
        return

    def listPluginModules(self):
        """
        Produce a list of plugin modules to be loaded

        Returns --

        - *list* : List of plugin module names
        
        """
        result = []
        for node in self.pluginModules().children:
            if node.name == "ShREEKPlugin":
                modName = node.attrs.get("Module", None)
                result.append(modName)
        return result
    
            

    def monitorConfiguration(self):
        """
        Return ref to monitor configuration IMProvNode
        """
        return self["ShREEKMonitors"][0]
    

    def listMonitorCfgs(self):
        """
        _listMonitorCfgs_

        Return a list of ShREEKMonitorCfg Objects contained
        within this Config Object
        """
        result = []
        for node in self.monitorConfiguration().children:
            if node.name != "ShREEKMonitorCfg":
                continue
            monCfg = ShREEKMonitorCfg()
            monCfg.load(node)
            result.append(monCfg)
        return result

    def addMonitorCfg(self, shreekMonitorCfg):
        """
        Add a monitor configuration.
        """
        if not isinstance(shreekMonitorCfg, ShREEKMonitorCfg):
            msg = "Tried to add non ShREEKMonitorCfg to Monitor\n"
            msg += "Coonfiguration. Argument to addMonitortCfg\n"
            msg += "must be an instance of ShREEK.ShREEKMonitorCfg\n"
            raise ShREEKException(msg, ClassInstance = self,
                                  BadObject = shreekMonitorCfg)

        mname = shreekMonitorCfg.monitorName()
        for item in self.monitorConfiguration().children:
            if item.name != "ShREEKMonitorCfg":
                continue
            if item.monitorName() == mname:
                msg = "Added Duplicate Named Monitor: %s\n" % mname
                msg += "Each ShREEKMonitorCfg object added to the "
                msg += "configuration\nmust have a unique name\n"
                raise ShREEKException(
                    msg, ClassInstance = self,
                    DuplicateObject = shreekMonitorCfg,
                    DuplicateName = mname,
                    DuplicateType = shreekMonitorCfg.monitorType())
        self.monitorConfiguration().addNode(shreekMonitorCfg)
        return
    
        
    

    def updatorConfiguration(self):
        """
        Return a list of dynamic updator configurations.
        """
        return self['ShREEKUpdators'][0]

    def listUpdators(self):
        """
        Return a list of ShREEKUpdatorCfg instances to be used
        """
        result = []
        for node in self.updatorConfiguration().children:
            if node.name != "ShREEKUpdatorCfg":
                continue
            newCfg = ShREEKUpdatorCfg()
            newCfg.load(node)
            result.append(newCfg)
        return result
    
    def addUpdator(self, updatorName):
        """add an Updator to be loaded"""
        self.updatorConfiguration().addNode(
            ShREEKUpdatorCfg(updatorName)
            )
        return


    def setTaskTree(self, shreekTask):
        """
        _setTaskTree_

        Include a ShREEKTask task tree in the Configuration object.
        The shreekTask instance provided should be the top of the
        task tree
        """
        if not isinstance(shreekTask, ShREEKTask):
            msg = "Tried to add non ShREEKTask to ShREEKConfig\n"
            msg += "Argument to setTaskTree\n"
            msg += "must be an instance of ShREEK.ShREEKTask\n"
            raise ShREEKException(msg, ClassInstance = self,
                                  BadObject = shreekTask)
        self._TaskTree = shreekTask
        return
    
    def taskTree(self):
        """
        Return the task tree so that it can be manipulated
        """
        return self._TaskTree    
            
        

    def save(self, filename):
        """
        _save_
        
        Write this Configuration out as an XML file to the
        file provided by the argument

        Args --

        - *filename* : Path where file will be created. Will overwrite any
        existing file
        
        """
        #  //
        # // Serialise the Task Tree
        #//
        if self._TaskTree == None:
            msg = "Error: Task Tree is empty\n"
            msg += "You must provide a set of ShREEKTasks to execute!\n"
            raise ShREEKException(msg, ClassInstance = self)
        
        taskNode = IMProvNode("ShREEKTaskTree")
        self.addNode(taskNode)
        taskNode.addNode(self._TaskTree.makeIMProv())
        
        #  //
        # // Save Config to file as XML Document
        #//
        doc = IMProvDoc("ShREEKConfiguration")
        doc.addNode(self)
        handle = open(filename, "w")
        handle.write(doc.makeDOMDocument().toprettyxml())
        handle.close()
        return


    def load(self, improvNode):
        """
        _load_

        Load vanilla IMProv node instance containing
        ShREEKConfig information into self

        Args --

        - *improvNode* : IMProvNode instance containing ShREEKConfig
        information to be loaded.
        
        """
        if improvNode.name != self.__class__.__name__:
            msg = "Attempted to load non SHREEKConfig IMProvNode\n"
            msg += "into a ShREEKConfig Object\n"
            msg += "improvNode argument must contain a ShREEKConfig "
            msg += "structure\n"
            raise ShREEKException(msg, ClassInstance = self)

        self.name = improvNode.name
        self.attrs = improvNode.attrs
        self.children = improvNode.children
        self.chardata = improvNode.chardata
        self.update(improvNode)

        #  //
        # // Deserialise the task tree
        #//
        taskQ = IMProvQuery("ShREEKConfig/ShREEKTaskTree/ShREEKTask")
        taskNodes = taskQ(self)
        if len(taskNodes) == 0:
            msg = "No Tasks found in ShREEKConfiguration:\n"
            msg += "Nothing to execute...\n"
            raise ShREEKException(msg, ClassInstance = self)
        self._TaskTree = ShREEKTask("treetop")
        self._TaskTree.populate(taskNodes[0])
        return self
        
        
    def loadFromFile(self, filename):
        """
        _loadFromFile_

        Load the configuration saved in the file provided
        into this Config Object
        """
        if not os.path.exists(filename):
            msg = "Error: Attempted to load non-existent file:\n"
            msg += "\t%s\n" % filename
            msg += "Argument must be a valid path\n"
            raise ShREEKException(msg, ClassInstance = self,
                                  MissingFile = filename)

        try:
            result = loadIMProvFile(filename)
        except StandardError, ex:
            msg = "Error Loading Configuration File:\n"
            msg += "\t%s\n" % filename
            msg += "This file must be a valid XML Document\n"
            msg += "Containing a saved ShREEKConfig Object\n"
            msg += str(ex)
            raise ShREEKException(msg, ClassInstance = self,
                                  Filename = filename,
                                  ExceptionInstance = ex)
        except SAXParseException, ex:
            msg = "Error Parsing Configuration File XML:\n"
            msg += "\t%s\n" % filename
            msg += str(ex)
            raise ShREEKException(msg, ClassInstance = self,
                                  Filename = filename,
                                  ExceptionInstance = ex)
        cfg = result.get('ShREEKConfig', [None])[0]
        if cfg == None:
            msg = "Cannot Find ShREEKConfig in XML File:\n"
            msg += "\t%s\n" % filename
            msg += "Expected to find a ShREEKConfig Element\n"
            raise ShREEKException(msg, ClassInstance = self,
                                  Filename = filename)
        
        self.load(cfg)
