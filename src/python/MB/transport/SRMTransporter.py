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

        return self.runCommand(command)

    def transportOut(self, mbInstance):
        """
        Handle Transport from Current to Target
        """
        commandMaker = _CommandFactory['srm']
        command = commandMaker.transportCurrentToTarget(mbInstance)

        return self.runCommand(command)

    def transportInOut(self, mbInstance):
        """
        Handle Transport from Source to Target
        """
        commandMaker = _CommandFactory['srm']
        command = commandMaker.transportSourceToTarget(mbInstance)
        return self.runCommand(command)
        

        

