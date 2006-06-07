#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_RFCPTransporter_

Transporter implementation for using rfcp 

"""
__version__ = "$Version$"
__revision__ = "$Id: RFCPTransporter.py,v 1.2 2006/06/07 15:56:33 evansde Exp $"
 


from MB.transport.Transporter import Transporter
from MB.transport.TransportException import TransportFailed
from MB.commandBuilder.CommandFactory import getCommandFactory


_CommandFactory = getCommandFactory()

class RFCPTransporter(Transporter):
    """
    _RFCPTransporter_

    Specialisation of Transporter for handling transfers
    via rfcp
    """
    
    def __init__(self):
        Transporter.__init__(self)


    def transportIn(self, mbInstance):
        """
        Handle Transport from Source to Memory values
        """
        commandMaker = _CommandFactory['rfcp']
        command = commandMaker.transportSourceToCurrent(mbInstance)
        return self.runCommand(command)

    
    def transportOut(self, mbInstance):
        """
        Handle Transport from Memory to Target values
        """
        commandMaker = _CommandFactory['rfcp']
        command = commandMaker.transportCurrentToTarget(mbInstance)
        return self.runCommand(command)

    def transportInOut(self, mbInstance):
        """
        Handle Transport from Source to Target values
        """
        commandMaker = _CommandFactory['rfcp']
        command = commandMaker.transportSourceToTarget(mbInstance)
        return self.runCommand(command)
        

    
