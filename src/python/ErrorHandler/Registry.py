#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for ErrorHandler handlers 

A handler implementation must be registered with a Unique name
with this registry by doing:

from ErrorHandler.Registry import registerHandler
registerHandler(objectRef, name)

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
    HandlerRegistry = {}

    def __init__(self):
        msg = "ErrorHandler.Registry should not be initialised"
        raise RuntimeError, msg
    

def registerHandler(objectRef, name):
    """
    _registerHandler_

    Register a new Handler with the name provided

    """
    if name in Registry.HandlerRegistry.keys():
        msg = "Duplicate Name used to registerHandler object:\n"
        msg += "%s already exists\n"
        raise RuntimeError, msg
    if not callable(objectRef):
        msg = "Object registered as a Handler is not callable:\n"
        msg += "Object registered as %s\n" % name
        msg += "The object must be a callable object, either\n"
        msg += "a function or class instance with a __call__ method\n"
        raise RuntimeError, msg

    Registry.HandlerRegistry[name] = objectRef

    return

def retrieveHandler(name):
    """
    _retrieveHandler_

    Get the Handler object mapped to the name provided

    """
    if name not in Registry.HandlerRegistry.keys():
        msg = "Name: %s not a registered Handler\n" % name
        msg += "No object registered with that name in ErrorHandler Registry"
        raise RuntimeError, msg
    return Registry.HandlerRegistry[name] 

