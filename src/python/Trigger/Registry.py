#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for ErrorAction handlers 

A handler implementation must be registered with a Unique name
with this registry by doing:

from ErrorAction.Registry import registerAction
registerAction(objectRef, name)

Where objectRef is a callable object and name is the name that
it will be registered with.

"""
import types


class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of handler object to
    handler name. Class level object provides singleton like behaviour
    
    """
    ActionRegistry = {}

    def __init__(self):
        msg = "Action Registry should not be initialised"
        raise RuntimeError, msg
    

def registerAction(objectRef, name):
    """
    _registerAction_

    Register a new Action with the name provided

    """
    if name in Registry.ActionRegistry.keys():
        msg = "Duplicate Name used to registerAction object:\n"
        msg += "%s already exists\n"
        raise RuntimeError, msg
    if not callable(objectRef):
        msg = "Object registered as a Action is not callable:\n"
        msg += "Object registered as %s\n" % name
        msg += "The object must be a callable object, either\n"
        msg += "a function or class instance with a __call__ method\n"
        raise RuntimeError, msg

    Registry.ActionRegistry[name] = objectRef

    return

def retrieveAction(name):
    """
    _retrieveAction_

    Get the Action object mapped to the name provided

    """
    if name not in Registry.ActionRegistry.keys():
        msg = "Name: %s not a registered Action\n" % name
        msg += "No object registered with that name in ErrorAction Registry"
        raise RuntimeError, msg
    return Registry.ActionRegistry[name] 

