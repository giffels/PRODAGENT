#!/usr/bin/env python
"""
_SRMTransporter_

Transporter implementation using SRM via the commandBuilder
tools for SRM
"""
import popen2
from MB.transport.Transporter import Transporter
from MB.commandBuilder.CommandFactory import getCommandFactory
from MB.transport.TransportException import TransportFailed

_CommandFactory = getCommandFactory()



class SRMTransporter(Transporter):
    """
    _SRMTransporter_

    Provide file transport functionality based on SRM
    """
    def __init__(self):
        Transporter.__init__(self)


    def transportIn(self, mbInstance):
        """
        Handle Transport from Source to Current values
        """
        commandMaker = _CommandFactory['srm']
        command = commandMaker.transportSourceToCurrent(mbInstance)

        pop = popen2.Popen4(command)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        if exitCode > 0:
            msg = "SRM Transport failed "
            msg += "Command Used: %s" % command
            raise TransportFailed(
                msg, ClassInstance = self,
                MetaBroker = mbInstance)
        return True

    def transportOut(self, mbInstance):
        """
        Handle Transport from Current to Target
        """
        commandMaker = _CommandFactory['srm']
        command = commandMaker.transportCurrentToTarget(mbInstance)

        pop = popen2.Popen4(command)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        if exitCode > 0:
            msg = "SRM Transport failed "
            msg += "Command Used: %s\n" % command
            msg += "Output:\n%s\n" % pop.fromchild.read()
            raise TransportFailed(
                msg, ClassInstance = self,
                MetaBroker = mbInstance)
        return True

    def transportInOut(self, mbInstance):
        """
        Handle Transport from Source to Target
        """
        commandMaker = _CommandFactory['srm']
        command = commandMaker.transportSourceToTarget(mbInstance)

        pop = popen2.Popen4(command)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        if exitCode > 0:
            msg = "SRM Transport failed "
            msg += "Command Used: %s" % command
            raise TransportFailed(
                msg, ClassInstance = self,
                MetaBroker = mbInstance)
        return True

        

