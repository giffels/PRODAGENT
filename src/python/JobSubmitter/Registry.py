#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for Job Submitters


Submitters can be Registered using:

from JobSubmitter.Registry import registerSubmitter
registerSubmitter(objectRef, name)

objectRef must be a class object that implements the Submitter interface


"""
import types


class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of Creator object to
    creator name. Class level object provides singleton like behaviour
    
    """
    SubmitterRegistry = {}

    def __init__(self):
        msg = "JobSubmitter.Registry should not be initialised"
        raise RuntimeError, msg
    



def registerSubmitter(objectRef, name):
    """
    _registerSubmitter_

    Register a new Submitter class using the name provided.

    objectRef must be a reference to the class object that implements the
    SubmitterInterface

    name is the name used to retrieve the submitter object
    
    """
    if name in Registry.SubmitterRegistry.keys():
        msg = "Duplicate Name used to registerSubmitter object:\n"
        msg += "%s already exists\n" % name
        raise RuntimeError, msg

    if type(objectRef) != types.ClassType:
        msg = "Object Registered to Submitter Registry is not a class object\n"
        msg += "Object must be a Class object that implements the"
        msg += "SubmitterInterface\n"
        raise RuntimeError, msg

    Registry.SubmitterRegistry[name] = objectRef
    return


def retrieveSubmitter(name):
    """
    _retrieveSubmitter_

    Retrieve an instance of the submitter class that matches the name provided
    
    """
    
    if name not in Registry.SubmitterRegistry.keys():
        msg = "Name: %s not a registered Submitter\n" % name
        msg += "No object registered with that name in Submitter Registry"
        raise RuntimeError, msg
    return Registry.SubmitterRegistry[name]() 
    
