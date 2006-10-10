#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for Bots


"""
import types
import logging

class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of Bot object to
    Bot name. Class level object provides singleton like behaviour
    
    """
    BotRegistry = {}
   
    def __init__(self):
        msg = "AdminControl.Registry should not be initialised"
        raise RuntimeError, msg
    

def registerBot(objectRef, name):
    """
    _registerBot_

    Register a new Bot with the name provided

    """
    if name in Registry.BotRegistry.keys():
        msg = "Duplicate Name used to registerBot object:\n"
        msg += "%s already exists\n"
        raise RuntimeError, msg
    if not type(objectRef) == types.ClassType:
        msg = "WARNING: Bot Plugin named: %s\n" % name
        msg += "is not a Class Type Object. Bot Plugins should be "
        msg += "registered as Class objects that implement the "
        msg += "AdminControl.Bots.BotInterface API"
        logging.error(msg)
        raise RuntimeError, msg

    Registry.BotRegistry[name] = objectRef

    return

def retrieveBot(name):
    """
    _retrieveBot_

    Get the Bot object mapped to the name provided

    """
    if name not in Registry.BotRegistry.keys():
        msg = "Name: %s not a registered Bot\n" % name
        msg += "No object registered with that name in AdminControl Registry"
        raise RuntimeError, msg
    #  //
    registeredObject = Registry.BotRegistry[name]
    return registeredObject()


