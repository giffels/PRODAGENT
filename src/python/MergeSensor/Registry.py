#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for Merge Plugins

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of object to
    object name. Class level object provides singleton like behaviour
    
    """
    MergeRegistry = {}
   
    def __init__(self):
        msg = "Merge.Registry should not be initialised"
        raise RuntimeError, msg
    

def registerMergePolicy(objectRef, name):
    """
    _registerMergePolicy_

    Register a new merge policy with the name provided

    """
    if name in Registry.MergeRegistry.keys():
        msg = "Duplicate Name used to registerMergePolicy object:\n"
        msg += "%s already exists\n"
        raise RuntimeError, msg
   

    Registry.MergeRegistry[name] = objectRef

    return

def retrieveMergePolicy(name):
    """
    _retrieveMergePolicy_

    Get the Merge object mapped to the name provided

    """
    if name not in Registry.MergeRegistry.keys():
        msg = "Name: %s not a registered merge policy plugin\n" % name
        msg += "No object registered with that name in Merge Policy Registry"
        raise RuntimeError, msg
    registeredObject = Registry.MergeRegistry[name]
   
    return registeredObject()
    


