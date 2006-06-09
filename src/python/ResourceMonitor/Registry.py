#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for Resource Monitor plugins

A Monitor implementation must be registered with a Unique name
with this registry by doing:

from ResourceMonitor.Registry import registerMonitor
registerMonitor(objectRef, name)

Where objectRef is a class object that implements the MonitorInterface


"""
import types
import logging

class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of Creator object to
    creator name. Class level object provides singleton like behaviour
    
    """
    MonitorRegistry = {}
   
    def __init__(self):
        msg = "ResourceMonitor.Registry should not be initialised"
        raise RuntimeError, msg
    

def registerMonitor(objectRef, name):
    """
    _registerMonitor_

    Register a new Monitor with the name provided

    """
    if name in Registry.MonitorRegistry.keys():
        msg = "Duplicate Name used to registerMonitor object:\n"
        msg += "%s already exists\n"
        logging.error(msg)
        raise RuntimeError, msg
    if not type(objectRef) == types.ClassType:
        msg = "Error: Monitor Plugin named: %s\n" % name
        msg += "is not a Class Type Object. Monitor Plugins should be "
        msg += "registered as Class objects that implement the "
        msg += "ResourceMonitor.Monitors.MonitorInterface API"
        logging.error(msg)
        raise RuntimeError, msg

    Registry.MonitorRegistry[name] = objectRef

    return

def retrieveMonitor(name):
    """
    _retrieveMonitor_

    Get the Monitor object mapped to the name provided

    """
    if name not in Registry.MonitorRegistry.keys():
        msg = "Name: %s not a registered Monitor\n" % name
        msg += "No object registered with that name in ResourceMonitor Registry"
        logging.error(msg)
        raise RuntimeError, msg
    registeredObject = Registry.MonitorRegistry[name]
    return registeredObject()



