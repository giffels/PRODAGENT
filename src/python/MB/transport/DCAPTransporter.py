#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_DCAPTransporter_

Transporter implementation for using dcap

"""
__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: DCAPTransporter.py,v 1.2 2006/03/02 21:54:41 evansde Exp $"
 

import popen2
import time

from MB.transport.Transporter import Transporter
from MB.transport.TransportException import TransportFailed
from MB.commandBuilder.CommandFactory import getCommandFactory
_CommandFactory = getCommandFactory()

class DCAPTransporter(Transporter):
    """
    _DCAPTransporter_

    Specialisation of Transporter for handling transfers
    via dcap 
    """
    
    def __init__(self):
        Transporter.__init__(self)


    def transportIn(self, mbInstance):
        """
        Handle Transport from Source to Memory values
        """
        commandMaker = _CommandFactory['dccp']
        command = commandMaker.transportSourceToCurrent(mbInstance)
        
        pop = popen2.Popen4(command)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        if exitCode > 0:
            msg = "DCAP Transport failed "
            msg += "Command Used: %s" % command
            raise TransportFailed(
                msg, ClassInstance = self,
                CommandOutput = pop.fromchild.read(),
                MetaBroker = mbInstance)
        return True
    
    def transportOut(self, mbInstance):
        """
        Handle Transport from Memory to Target values
        """
        commandMaker = _CommandFactory['dccp']
        command = commandMaker.transportCurrentToTarget(mbInstance)

        pop = popen2.Popen4(command)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        if exitCode > 0:
            msg = "DCAP Transport failed "
            msg += "Command Used: %s" % command
            raise TransportFailed(
                msg, ClassInstance = self,
                CommandOutput = pop.fromchild.read(),
                MetaBroker = mbInstance)
        return True

    
    def transportInOut(self, mbInstance):
        """
        Handle Transport from Source to Target values
        """
        commandMaker = _CommandFactory['dccp']
        command = commandMaker.transportSourceToTarget(mbInstance)
        
        pop = popen2.Popen4(command)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        if exitCode > 0:
            msg = "DCAP Transport failed "
            msg += "Command Used: %s" % command
            raise TransportFailed(
                msg, ClassInstance = self,
                CommandOutput = pop.fromchild.read(),
                MetaBroker = mbInstance)
        return True

    

    
