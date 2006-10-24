#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for Prioritiser plugins


"""
import types
import logging

class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of plugin object to
    plugin name. Class level object provides singleton like behaviour
    
    """
    PrioritiserRegistry = {}
   
    def __init__(self):
        msg = "JobQueue.Registry should not be initialised"
        raise RuntimeError, msg
    

def registerPrioritiser(objectRef, name):
    """
    _registerPrioritiser_

    Register a new Prioritiser with the name provided

    """
    if name in Registry.PrioritiserRegistry.keys():
        msg = "Duplicate Name used to registerPrioritiser object:\n"
        msg += "%s already exists\n"
        raise RuntimeError, msg
 

    Registry.PrioritiserRegistry[name] = objectRef

    return

def retrievePrioritiser(name):
    """
    _retrievePrioritiser_

    Get the Prioritiser object mapped to the name provided

    """
    if name not in Registry.PrioritiserRegistry.keys():
        msg = "Name: %s not a registered Prioritiser\n" % name
        msg += "No object registered with that name in Prioritiser Registry"
        raise RuntimeError, msg
    registeredObject = Registry.PrioritiserRegistry[name]
    return registeredObject()


