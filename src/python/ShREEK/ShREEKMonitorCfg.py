#!/usr/bin/python
"""
_ShREEKMonitorCfg_

Configuration Container for a ShREEKMonitor Configuration
based on an IMProvNode

Also contains an IMProvNode derived container for an Updator
to be used while processing.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ShREEKMonitorCfg.py,v 1.1 2005/12/30 18:54:25 evansde Exp $"
__author__ = "evansde@fnal.gov"

from IMProv.IMProvNode import IMProvNode

from ShREEK.ShREEKException import ShREEKException

class ShREEKMonitorCfg(IMProvNode):
    """
    _ShREEKMonitorCfg_

    Monitor Configuration Container.
    Contains information for configuring a particular
    monitor object.
    Includes monitor name and type at minimum and
    also a set of optional arguments to be used
    to configure the monitor
    """
    def __init__(self, **initArgs):
        IMProvNode.__init__(self, self.__class__.__name__,
                            **initArgs)
        self.addNode(IMProvNode("PositionalArgs"))
        self.addNode(IMProvNode("KeywordArgs"))
        
        

    def setMonitorName(self, name):
        """
        _setMonitorName_

        Set the Name of the Monitor Instance
        """
        self.attrs["MonitorName"] = name
        return

    def setMonitorType(self, monitorType):
        """
        _setMonitorType_

        Set the Monitor Type
        """
        self.attrs["MonitorType"] = monitorType
        return

    def monitorName(self):
        """
        Return the name of the monitor to be configured
        """
        return self.attrs.get("MonitorName", None)

    def monitorType(self):
        """
        Return the type of the monitor to be configured
        """
        return self.attrs.get("MonitorType", None)

    def positionalArgs(self):
        """
        Return the list of positional args for configuring
        the monitor
        """
        result = []
        posArgs = self['PositionalArgs'][0]
        for item in posArgs.children:
            if item.name == "PositionalArg":
                result.append(item.attrs.get("Value", None))
        return result
    

    def addPositionalArg(self, *args):
        """
        _addPositionalArg_

        Add a set of positional args to this configuration
        """
        for item in args:
            node = IMProvNode("PositionalArg", Value = item)
            self['PositionalArgs'][0].addNode(node)
        return
            
        

    def keywordArgs(self):
        """
        Return the Dictionary of Keyword:Value args
        for configuring the Monitor
        """
        result = {}
        kwArgs = self['KeywordArgs'][0]
        for item in kwArgs.children:
            if item.name == "KeywordArg":
                resultKey = str(item.attrs.get("Key", None))
                resultVal = item.attrs.get("Value", None)
                result[resultKey] = resultVal
        return result
        
    def addKeywordArg(self, **args):
        """
        _addKeywordArg_

        Add a set of keyword = value args to this task
        configuration
        """
        for key, val in args.items():
            node = IMProvNode("KeywordArg",
                              Key = key, Value = val)
            self['KeywordArgs'][0].addNode(node)
        return
        
        
    def load(self, improvNode):
        """
        _load_

        Convert a Vanilla IMProvNode instance into
        a ShREEKMonitorCfg object by copying value into
        self
        """
        if improvNode.name != self.__class__.__name__:
            msg = "Tried to load non ShREEKMonitorCfg Node\n"
            msg += "into ShREEKMonitorCfg object\n"
            raise ShREEKException(msg, ClassInstance = self,
                                  BadNode = improvNode,
                                  BadNodeName = improvNode.name)
   
        self.name = improvNode.name
        self.attrs = improvNode.attrs
        self.chardata = improvNode.chardata
        self.children = improvNode.children
        self.update(improvNode)
      
        return self



class ShREEKUpdatorCfg(IMProvNode):
    """
    _ShREEKUpdatorCfg_

    Object to represent a requirement for a ShREEK Updator
    within a configuration
    """
    def __init__(self, updatorName = None):
        IMProvNode.__init__(self, self.__class__.__name__)
        self.attrs['Name'] = updatorName


    def updatorName(self):
        """return updator name"""
        return str(self.attrs['Name'])

    def setUpdatorName(self, name):
        """set updator name"""
        self.attrs['Name'] = name

    
    def load(self, improvNode):
        """
        _load_

        Convert a Vanilla IMProvNode instance into
        a ShREEKUpdatorCfg object by copying value into
        self
        """
        if improvNode.name != self.__class__.__name__:
            msg = "Tried to load non ShREEKUpdatorCfg Node\n"
            msg += "into ShREEKUpdatorCfg object\n"
            raise ShREEKException(msg, ClassInstance = self,
                                  BadNode = improvNode,
                                  BadNodeName = improvNode.name)
   
        self.name = improvNode.name
        self.attrs = improvNode.attrs
        return self
    
