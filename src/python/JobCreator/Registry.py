#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for JobCreator Creators

A Creator implementation must be registered with a Unique name
with this registry by doing:

from JobCreator.Registry import registerCreator
registerCreator(objectRef, name)

Where objectRef is a callable object and name is the name that
it will be registered with.

The name of the Creator to be used within a particular prodAgent installation
is provided to the JobCreator component from the config file and should
match the name of one of the registered creators.



"""
import types


class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of Creator object to
    creator name. Class level object provides singleton like behaviour
    
    """
    CreatorRegistry = {}
   
    def __init__(self):
        msg = "JobCreator.Registry should not be initialised"
        raise RuntimeError, msg
    

def registerCreator(objectRef, name):
    """
    _registerCreator_

    Register a new Creator with the name provided

    """
    if name in Registry.CreatorRegistry.keys():
        msg = "Duplicate Name used to registerCreator object:\n"
        msg += "%s already exists\n"
        raise RuntimeError, msg
    if not callable(objectRef):
        msg = "Object registered as a Creator is not callable:\n"
        msg += "Object registered as %s\n" % name
        msg += "The object must be a callable object, either\n"
        msg += "a function or class instance with a __call__ method\n"
        raise RuntimeError, msg

    Registry.CreatorRegistry[name] = objectRef

    return

def retrieveCreator(name):
    """
    _retrieveCreator_

    Get the Creator object mapped to the name provided

    """
    if name not in Registry.CreatorRegistry.keys():
        msg = "Name: %s not a registered Creator\n" % name
        msg += "No object registered with that name in JobCreator Registry"
        raise RuntimeError, msg
    return Registry.CreatorRegistry[name] 


