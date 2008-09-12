#!/usr/bin/env python
"""
_ShLoggerRegistry_

Registry Module for Log Adapter plugins

"""
__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: ShLoggerRegistry.py,v 1.2 2006/01/09 20:27:21 evansde Exp $"
__author__ = "evansde@fnal.gov"

import types
import inspect

from ShLogger.ShLoggerException import ShLoggerException
from ShLogger.ShLoggerAdapter import ShLoggerAdapter



def registerShLoggerAdapter(classRef, name = None):
    """
    _registerShLoggerAdapter_
    
    Register a ShLoggerAdapter Class Object with the
    name provided. If the name arg is None, then the
    class Name is used instead.
    
    Args --
    
    - *classRef* : Class Object to be registered as a Log
    Adapter, must be a ClassType object that inherits from
    ShLoggerAdapter.
    
    """
    if type(classRef) != types.ClassType:
        msg = "Object is not a ClassType:\n"
        msg += "%s\n" % classRef
        msg += "Argument must be a ClassType instance"
        raise ShLoggerException(msg,
                                BadArgument = classRef)
                                
    classNames = []
    for item in inspect.getmro(classRef) :
        classNames.append(item.__name__)
    
    
    if ShLoggerAdapter.__name__ not in classNames:
        msg = "Attempted to register an adapter that is not\n"
        msg += "of the ShLoggerAdapter class:\n"
        msg += str(classRef)
        raise ShLoggerException(msg,
                                BadArgument = classRef)
    regName = name or classRef.__name__
    setattr(ShLoggerRegistry, regName, classRef)
    return
    
    
def loadShLoggerAdapter(name):
    """
    _loadShLoggerAdapter_

    Return an instance of the ShLoggerAdapter registered
    by name in the ShLoggerRegistry

    Args --

    - *name* : Name of the Registered Object

    Returns --

    - *ShLoggerAdapter* : Instance of the requested adapter
    
    """
    classRef = getattr(ShLoggerRegistry, name, None)
    if classRef == None:
        msg = "Object Named %s is not registered as a ShLoggerAdapter" % name
        raise ShLoggerException(msg, MissingObject = name)
    return classRef()

class ShLoggerRegistry:
    """
    _ShLoggerRegistry_

    Namespace registry for ShLogger Adapter objects.
    Access via the registerShLoggerAdapter and
    loadShLoggerAdapter methods in this module.

    """
    def __init__(self):
        msg = "ShLoggerRegistry is a Namespace Object"
        msg += "and should not be instantiated"
        raise RuntimeError, msg



