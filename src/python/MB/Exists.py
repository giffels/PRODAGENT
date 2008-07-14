#!/usr/bin/env python
"""
_Exists_

Existence test methods for MetaBroker Objects

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: Exists.py,v 1.1 2005/12/30 18:51:37 evansde Exp $"

import popen2

from MB.query.QueryFactory import getQueryFactory
from MB.commandBuilder.CommandFactory import getCommandFactory

_QueryFactory = getQueryFactory()
_CommandFactory = getCommandFactory()

def exists(mbInstance):
    """
    _Exists_

    Straightforward existence check on
    a MetaBroker object using the
    builtin QueryMethod field

    """
    
    queryMethod = _QueryFactory[mbInstance]
    try:
        result = queryMethod(mbInstance)
    except StandardError, ex:
        result = False

    return result
    
    
def sourceExists(mbInstance, outputHandle = None):
    """
    _sourceExists_

    Use QueryMethod key to create a sourceExists command
    for the metabroker instance provided and execute the existence
    check command

    Args --

    - *mbInstance* : MetaBroker instance to be queried

    - *outputHandle* : Optional file object to write output of command
    to
    
    """
    queryMethod = mbInstance["QueryMethod"]
    commandMaker = _CommandFactory[queryMethod]
    command = commandMaker.sourceExists(mbInstance)
    pop = popen2.Popen4(command)
    while pop.poll() == -1:
        exitCode = pop.poll()
    exitCode = pop.poll()
    if outputHandle != None:
        outputHandle.write(pop.fromchild.read())
    if exitCode > 0:
        return False
    return True

def currentExists(mbInstance, outputHandle = None):
    """
    _currentExists_

    Use QueryMethod and commandBuilder to check existence of the
    current metaBroker values

    Args --

    - *mbInstance* : MetaBroker instance to be queried

    - *outputHandle* : Optional file object to write output of command
    to
    
    """
    queryMethod = mbInstance["QueryMethod"]
    commandMaker = _CommandFactory[queryMethod]
    command = commandMaker.currentExists(mbInstance)
    pop = popen2.Popen4(command)
    while pop.poll() == -1:
        exitCode = pop.poll()
    exitCode = pop.poll()
    if outputHandle != None:
        outputHandle.write(pop.fromchild.read())
    if exitCode > 0:
        return False
    return True

def targetExists(mbInstance, outputHandle = None):
    """
    _targetExists_

    Use QueryMethod and commandBuilder to check existence of the
    target metaBroker values

    Args --

    - *mbInstance* : MetaBroker instance to be queried

    - *outputHandle* : Optional file object to write output of command
    to    
    
    """
    queryMethod = mbInstance["QueryMethod"]
    commandMaker = _CommandFactory[queryMethod]
    command = commandMaker.targetExists(mbInstance)
    pop = popen2.Popen4(command)
    while pop.poll() == -1:
        exitCode = pop.poll()
    exitCode = pop.poll()
    if outputHandle != None:
        outputHandle.write(pop.fromchild.read())
    if exitCode > 0:
        return False
    return True
