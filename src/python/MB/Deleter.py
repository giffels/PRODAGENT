#/usr/bin/env python
"""
_Deleter_

Find the appropriate commandBuilder based on the TransportMethod protocol and invoke the deleteCurrent method
in a popen

"""

import popen2
from MB.MBException import MBException
from MB.commandBuilder.CommandFactory import getCommandFactory


_Factory = getCommandFactory()



def delete(mbInstance):
    """
    _delete_


    """

    protocol = mbInstance['TransportMethod']
    command = _Factory[protocol](mbInstance)

    pop = popen2.Popen4(command)
    while pop.poll() == -1:
        exitCode = pop.poll()
    exitCode = pop.poll()
    if exitCode:
        msg = "Deletion failed: Exit %s\n" % exitCode
        msg += pop.fromchild.read()
        raise MBException(msg, ClassInstance = self,
                          MBInstance = mbInstance,
                          Command = command)
    return
