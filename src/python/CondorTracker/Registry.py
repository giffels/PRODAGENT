#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for Tracker Plugins



"""
import types
import logging

class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of Creator object to
    creator name. Class level object provides singleton like behaviour
    
    """
    TrackerRegistry = {}
   
    def __init__(self):
        msg = "CondorTracker.Registry should not be initialised"
        raise RuntimeError, msg
    

def registerTracker(objectRef, name):
    """
    _registerTracker_

    Register a new Tracker with the name provided

    """
    if name in Registry.TrackerRegistry.keys():
        msg = "Duplicate Name used to registerTracker object:\n"
        msg += "%s already exists\n"
        raise RuntimeError, msg
   

    Registry.TrackerRegistry[name] = objectRef

    return

def retrieveTracker(name):
    """
    _retrieveTracker_

    Get the Tracker object mapped to the name provided

    """
    if name not in Registry.TrackerRegistry.keys():
        msg = "Name: %s not a registered Tracker\n" % name
        msg += "No object registered with that name in CondorTracker Registry"
        raise RuntimeError, msg
    registeredObject = Registry.TrackerRegistry[name]
   
    return registeredObject()
    


