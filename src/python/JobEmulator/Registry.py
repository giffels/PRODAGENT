#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module

"""
__revision__ = "$Id: "
__version__ = "$Revision: "

import types
import logging

class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of plugin object to
    plugin name. Class level object provides singleton like behaviour
    
    """
    _Registry = {}
   
    def __init__(self):
        msg = "Registry should not be initialised"
        raise RuntimeError, msg
    

def registerPlugin(objectRef, name):
    """
    _registerPlugin_

    Register a new Plugin with the name provided

    """

    if name in Registry._Registry.keys():
        msg = "Duplicate Name used to registerPlugin object:\n"
        msg += "%s already exists\n"
        raise RuntimeError, msg

    Registry._Registry[name] = objectRef

    return

def retrievePlugin(name):
    """
    _retrievePlugin_

    Get the Plugin object mapped to the name provided

    """

    if name not in Registry._Registry.keys():
        msg = "Name: %s not a registered Plugin\n" % name
        msg += "No object registered with that name in Plugin Registry"
        logging.error(msg)
        raise RuntimeError, msg
    
    registeredObject = Registry._Registry[name]
    return registeredObject()


