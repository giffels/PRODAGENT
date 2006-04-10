#!/usr/bin/env python
"""
_ControlPointFactory_

Registry and Factory Methods for ControlPoint
components.

Conditionals and Actions must be registered with this
Factory by the methods provided.

"""
__revision__ = "$Id: ControlPointFactory.py,v 1.1 2005/12/30 18:54:26 evansde Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "evansde@fnal.gov"


import types
from ShREEK.ShREEKException import ShREEKException
from ShREEK.ControlPoints.Conditional import Conditional
from ShREEK.ControlPoints.Action import Action


def registerConditional(condClassRef):
    """
    _registerConditional_

    Register a Class Reference for a Conditional.
    The argument must be a reference to a Conditional
    derived Class Object (not an instance).

    """
    if type(condClassRef) != types.ClassType:
        msg = "Attempted to Register Non-Class Type\n"
        msg += "With ConditionalRegistry\n"
        msg += "Argument must be a Class Reference\n"
        raise ShREEKException(msg, BadArgument = condClassRef)
    
    if not issubclass(condClassRef, Conditional):
        msg = "Attempted to Register Non-Conditional Class\n"
        msg += "With ConditionalRegistry\n"
        msg += "Argument must be a subclass of Conditional\n"
        raise ShREEKException(msg, BadArgument = condClassRef)

    setattr(ConditionalRegistry, condClassRef.__name__, condClassRef)
    return


def registerAction(actionClassRef):
    """
    _registerAction_

    Register a Class Reference for an Action.
    The argument must be a reference to an Action
    derived Class Object (not an instance).

    """
    if type(actionClassRef) != types.ClassType:
        msg = "Attempted to Register Non-Class Type\n"
        msg += "With ActionRegistry\n"
        msg += "Argument must be a Class Reference\n"
        raise ShREEKException(msg, BadArgument = actionClassRef)
    
    if not issubclass(actionClassRef, Action):
        msg = "Attempted to Register Non-Action Class\n"
        msg += "With ActionRegistry\n"
        msg += "Argument must be a subclass of Action\n"
        raise ShREEKException(msg, BadArgument = actionClassRef)

    setattr(ActionRegistry, actionClassRef.__name__, actionClassRef)
    return

def newConditional(condName):
    """
    _newConditional_

    Instantiate a new Conditional Instance
    based on the condName argument.
    Lookup the class name in the ConditionalRegistry
    and return a new Instance of that class.
    If the Conditional is not registered, then
    a ShREEKException will be thrown.
    """
    classRef = getattr(ConditionalRegistry, condName, None)
    if classRef == None:
        msg = "Unknown Conditional: %s\n" % condName
        msg += "This name is not a class registered in "
        msg += "the ConditionalRegistry\n"
        condKeys = []
        for key in ConditionalRegistry.__dict__.keys():
            if not key.startswith('__'):
                condKeys.append(key)
        raise ShREEKException(
            msg, BadConditional = condName,
            ValidConditionals = condKeys) 

    return classRef()


def newAction(actionClass, actionName):
    """
    _newAction_

    Factory Method to instantiate a new Action Instance
    of class actionClass with Name attribute actionName
    Lookup the class in ActionRegistry and return an instance
    of the object with the actionName passed to the ctor.
    """
    classRef = getattr(ActionRegistry, actionClass, None)
    if classRef == None:
        msg = "Unknown Action: %s with Name %s\n" % (
            actionClass, actionName,
            )
        msg += "This name is not a class registered in "
        msg += "the ActionRegistry\n"
        actKeys = []
        for key in ActionRegistry.__dict__.keys():
            if not key.startswith('__'):
                actKeys.append(key)
        raise ShREEKException(
            msg, BadAction = actionClass,
            ActionName = actionName,
            ValidActions = actKeys) 

    return classRef(actionName)

class ConditionalRegistry:
    """
    _ConditionalRegistry_

    Namespace for containing references to
    Conditional Classes so that they can be instantiated
    by the conditionalFactory Method.

    """
    def __init__(self):
        msg = "ConditionalRegistry is a namespace\n"
        msg += "And should not be instantiated\n"
        raise RuntimeError, msg



class ActionRegistry:
    """
    _ActionRegistry_

    Namespace for containing references to
    Action classes so that they can be instantiated
    by the actionFactory method
    """
    def __init__(self):
        msg = "ActionRegistry is a namespace\n"
        msg += "And should not be instantiated\n"
        raise RuntimeError, msg

    
    
