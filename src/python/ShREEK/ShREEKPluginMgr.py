#!/usr/bin/env python
"""
_ShREEKPluginMgr_

Manager and loader class for finding and loading
ShREEK Plugin modules.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ShREEKPluginMgr.py,v 1.1 2006/04/10 17:38:42 evansde Exp $"

import inspect

from types import ClassType
from types import InstanceType
from types import MethodType
from types import FunctionType

from ShREEK.ShREEKException import ShREEKException


def _InheritsFrom(classObject, className):
    """
    _InheritsFrom_
    
    Make sure classObject either is, or inherits
    a class named className
    
    """
    classObjects = inspect.getmro(classObject)
    for item in classObjects:
        if item.__name__ == className:
            return True
    return False


def _ObjectName(objectRef):
    """
    _ObjectName_

    Return the name of objectRef based on Type

    """
    typeVal = type(objectRef)
    if typeVal == ClassType:
        return str(objectRef.__name__)
    if typeVal == MethodType:
        className = str(objectRef.im_self.__class__.__name__)
        methName = str(objectRef.im_func.__name__)
        return "%s.%s" % (className, methName)
    if typeVal == FunctionType:
        return str(objectRef.__name__)
    if typeVal == InstanceType:
        return str(objectRef.__class__.__name__)
    #  //
    # // Shouldnt get here...
    #//
    return str(objectRef)
    
        

def registerShREEKMonitor(monitorRef, name = None):
    """
    _registerShREEKMonitor_

    Function to register a monitor instance
    as a ShREEK Plugin

    Args --

    - *monitorRef* : Monitor Class Object to be registered, this class
    must be a ClassType (ie not an instance, it will be instantiated on demand)
    and must inherit from ShREEKMonitor

    - *name* : String, name of the monitor to be registered under. If not
    provided this will default to the name of the class
    
    """
    if type(monitorRef) != ClassType:
        msg = "Attempted to Register Monitor that is not a Class Type\n"
        msg += "registerShREEKMonitor argument must be a reference to a\n"
        msg += "class, not an instance"
        raise ShREEKException(
            msg, BadMonitor = monitorRef,
            MonitorName = name)
    if not _InheritsFrom(monitorRef, "ShREEKMonitor"):
        msg = "Attempted to Register Monitor that is not a subclass\n"
        msg += "of ShREEKMonitor:"
        msg += str(monitorRef)
        raise ShREEKException(
            msg, BadMonitor = monitorRef,
            MonitorName = name)
    objectName = name
    if name == None:
        objectName = str(monitorRef.__name__)
    ShREEKPlugins._ShREEKMonitors[objectName] = monitorRef
    return


def registerShREEKUpdator(updatorRef, name = None):
    """
    _registerShREEKUpdator_
    
    Function to register an updator object
    as a ShREEK plugin that will be updated
    
    """
    objectName = name
    if name == None:
        objectName = _ObjectName(updatorRef)
    if not callable(updatorRef):
        msg = "Attempted to Register uncallable Updator\n"
        msg += "Updators must be callable objects:\n"
        msg += str(updatorRef)
        raise ShREEKException(
            msg, BadUpdator = updatorRef,
            UpdatorName = objectName)
    

    ShREEKPlugins._ShREEKUpdators[objectName] = updatorRef
    return
    
    
def registerShREEKControlPoint(controlPointRef):
    """
    _registerShREEKControlPoint_

    Function to register a control point instance
    as a ShREEK plugin
    """
    raise NotImplementedError, "registerShREEKControlPoint: %s" % (
        controlPointRef,
        )

def listMonitorPlugins():
    """
    _listMonitors_

    provide a list of monitor names registered with the
    Plugin Manager
    """
    return ShREEKPlugins._ShREEKMonitors.keys()

def listUpdatorPlugins():
    """
    _listUpdators_

    Return the list of dynamic updators registered with the plugin
    manager
    """
    return ShREEKPlugins._ShREEKUpdators.keys()


def getMonitorPlugin(name):
    """
    _getMonitorPlugin_

    Return a Monitor instance by name, return None if name isnt
    registered
    
    """
    monitorClass = ShREEKPlugins._ShREEKMonitors.get(name, None)
    if monitorClass != None:
        monitorClass = monitorClass()
    return monitorClass


def getUpdatorPlugin(name):
    """
    _getUpdatorPlugin_

    First search the dynamic updators, then the static updators
    and return the reference stored for the name.
    return None if the name is not registered

    """
    updatorRef = ShREEKPlugins._ShREEKUpdators.get(name, None)
    return updatorRef

def resetShREEKPlugins():
    """reset the plugin tables, this should only be used for unittests"""
    ShREEKPlugins._ShREEKMonitors = {}
    ShREEKPlugins._ShREEKUpdators = {}
    ShREEKPlugins._ShREEKControlPoints = {}
    return
    
class ShREEKPlugins:
    """
    Plugin Registry class that has plugin modules
    registered to it so that they can be loaded
    when required

    """
    def __init__(self):
        msg = "ShREEKPlugins is a namespace class"
        msg += "and should not be explicitly instantiated"
        raise RuntimeError, msg

    

    _ShREEKMonitors = {}

    _ShREEKUpdators = {}
    _ShREEKControlPoints = {}
    
    
    listUpdators = staticmethod(listUpdatorPlugins)
    listMonitors = staticmethod(listMonitorPlugins)
    
    
    getMonitor = staticmethod(getMonitorPlugin)
    getUpdator = staticmethod(getUpdatorPlugin)
    
