#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_RFIOTransporter_

Transporter implementation for using rfio 

"""
__version__ = "$Version$"
__revision__ = "$Id: RFIOTransporter.py,v 1.1 2006/04/10 17:10:00 evansde Exp $"
 


from MB.transport.Transporter import Transporter
from MB.transport.TransportException import TransportFailed
from MB.commandBuilder.CommandFactory import getCommandFactory


_CommandFactory = getCommandFactory()

class RFIOTransporter(Transporter):
    """
    _RFIOTransporter_

    Specialisation of Transporter for handling transfers
    via rfio
    """
    
    def __init__(self):
        Transporter.__init__(self)


    def transportIn(self, mbInstance):
        """
        Handle Transport from Source to Memory values
        """
        commandMaker = _CommandFactory['rfio']
        command = commandMaker.transportSourceToCurrent(mbInstance)
        return self.runCommand(command)

    
    def transportOut(self, mbInstance):
        """
        Handle Transport from Memory to Target values
        """
        commandMaker = _CommandFactory['rfio']
        command = commandMaker.transportCurrentToTarget(mbInstance)
        return self.runCommand(command)

    def transportInOut(self, mbInstance):
        """
        Handle Transport from Source to Target values
        """
        commandMaker = _CommandFactory['rfio']
        command = commandMaker.transportSourceToTarget(mbInstance)
        return self.runCommand(command)
        

    
