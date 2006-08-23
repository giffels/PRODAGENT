#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_RFIOTransporter_

Transporter implementation for using rfio 

"""
__version__ = "$Version$"
__revision__ = "$Id: RFIOTransporter.py,v 1.2 2006/06/07 15:56:33 evansde Exp $"
 

import time
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
        Ntries = 1
        while Ntries<=3:
         print " Copy attempt number %i" %Ntries
         Ntries = Ntries+1
         try:
            res=self.runCommand(command)
         except TransportFailed:
           print "sleeping time 30sec"
           time.sleep(30)
           res = False
           continue
         break

        return res
        #return self.runCommand(command)



    def transportInOut(self, mbInstance):
        """
        Handle Transport from Source to Target values
        """
        commandMaker = _CommandFactory['rfio']
        command = commandMaker.transportSourceToTarget(mbInstance)
        return self.runCommand(command)
        

    
