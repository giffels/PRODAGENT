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
import logging

class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of Creator object to
    creator name. Class level object provides singleton like behaviour
    
    """
    CreatorRegistry = {}
    GeneratorRegistry = {}
    
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
    if not type(objectRef) == types.ClassType:
        #  //
        # // Futureproof warning, will change to a type check and
        #//  exception throw in the near future.
        msg = "WARNING: Creator Plugin named: %s\n" % name
        msg += "is not a Class Type Object. Creator Plugins should be "
        msg += "registered as Class objects that implement the "
        msg += "JobCreator.Creators.CreatorInterface API"
        logging.warning(msg)
        print msg

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
    #  //
    # // Temp check on type: If Class type, return an instance
    #//  if not return the object itself. Soon will be class type only
    registeredObject = Registry.CreatorRegistry[name]
    if type(registeredObject) == types.ClassType:
        return registeredObject()
    return registeredObject


def registerGenerator(classRef, name):
    """
    _regsiterGenerator_

    Register a JobGenerator Plugin Class with the name provided

    """
    if name in Registry.GeneratorRegistry.keys():
        msg = "Duplicate Name used to registerGenerator object:\n"
        msg += "%s already exists\n"
        raise RuntimeError, msg
    
    Registry.GeneratorRegistry[name] = classRef
    return


def retrieveGenerator(name):
    """
    _retrieveGenerator_

    Instantiate and return an instance of the Generator registered with the name provided

    """
    if name not in Registry.GeneratorRegistry.keys():
        msg = "Name: %s not a registered Generator\n" % name
        msg += "No object registered with that name in JobCreator Registry"
        raise RuntimeError, msg

    classRef = Registry.GeneratorRegistry[name]
    return classRef()

