#!/usr/bin/env python
"""
_CreateDir_

Invoke the createTargetDir method provided by the commandBuilder implementation
using the transport method to look up the command.

Some builders will return this as a null command

"""
from MB.MBException import MBException
from MB.commandBuilder.CommandFactory import getCommandFactory
_CommandFactory = getCommandFactory()


def createDirectory(mbInstance):
    """
    _createDirectory_

    Create the directory by looking up the command builder for
    the mbInstance based off the TransportMethod protocol

    """
    commandMaker = _CommandFactory[mbInstance['TransportMethod']]
    command = commandMaker.createTargetDir(mbInstance)

    pop = popen2.Popen4(command)
    pop.tochild.close()
    output = pop.fromchild.read()
    exitCode = pop.wait()
        
    if exitCode > 0:
        msg = "createDirectory failed for %s\n" % (
            self.__class__.__name__,
            )
        msg += "Command Used: %s" % command
        raise MBException(
            msg, ClassInstance = self,
            Command = command)
    return 0
    
    
    
