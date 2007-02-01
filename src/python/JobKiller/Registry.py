#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for Killer Plugins



"""
import types
import logging

class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of object to
    object name. Class level object provides singleton like behaviour
    
    """
    KillerRegistry = {}
   
    def __init__(self):
        msg = "JobKiller.Registry should not be initialised"
        raise RuntimeError, msg
    

def registerKiller(objectRef, name):
    """
    _registerKiller_

    Register a new Killer with the name provided

    """
    if name in Registry.KillerRegistry.keys():
        msg = "Duplicate Name used to registerKiller object:\n"
        msg += "%s already exists\n"
        raise RuntimeError, msg
   

    Registry.KillerRegistry[name] = objectRef

    return

def retrieveKiller(name):
    """
    _retrieveKiller_

    Get the Killer object mapped to the name provided

    """
    if name not in Registry.KillerRegistry.keys():
        msg = "Name: %s not a registered Killer\n" % name
        msg += "No object registered with that name in JobKiller Registry"
        raise RuntimeError, msg
    registeredObject = Registry.KillerRegistry[name]
   
    return registeredObject()
    


