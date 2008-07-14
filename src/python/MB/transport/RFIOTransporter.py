#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_RFIOTransporter_

Transporter implementation for using rfio 

"""
__version__ = "$Version$"
__revision__ = "$Id: RFIOTransporter.py,v 1.1 2005/12/30 18:51:41 evansde Exp $"
 

import popen2
import time

from MB.transport.Transporter import Transporter
from MB.transport.TransportException import TransportFailed

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
        source = mbInstance['Source']
        if mbInstance.isRemote():
            target = mbInstance['RemoteName']
        else:
            target = mbInstance['AbsName']
            
        args = self._ExtractOpts(mbInstance)
        try:
            self._Copy(source, target, **args)
        except TransportFailed, ex:
            ex.addInfo(MetaBroker = mbInstance)
            raise ex
        mbInstance['URL'] = "rfio:%s" % mbInstance['AbsName']
        return

    def transportOut(self, mbInstance):
        """
        Handle Transport from Memory to Target values
        """
        target = mbInstance['Target']
        if mbInstance.isRemote():
            source = mbInstance['RemoteName']
        else:
            source = mbInstance['AbsName']
            
        args = self._ExtractOpts(mbInstance)
        try:
            self._Copy(source, target, **args)
        except TransportFailed, ex:
            ex.addInfo(MetaBroker = mbInstance)
            raise ex
        mbInstance['URL'] = "rfio:%s:%s" % (
            mbInstance['TargetHostName'],
            mbInstance['TargetAbsName'],
            )
        return

    def transportInOut(self, mbInstance):
        """
        Handle Transport from Source to Target values
        """
        target = mbInstance['Target']
        source = mbInstance['Source']
            
        args = self._ExtractOpts(mbInstance)
        try:
            self._Copy(source, target, **args)
        except TransportFailed, ex:
            ex.addInfo(MetaBroker = mbInstance)
            raise ex
        mbInstance['URL'] = "rfio:%s:%s" % (
            mbInstance['TargetHostName'],
            mbInstance['TargetAbsName'],
            )

        return

    

    def _ExtractOpts(self, mbInstance):
        """
        Extract any extra options for RFIO Transfer
        from the metabroker instance and construct the
        args dictionary
        """
        args = {}
        if mbInstance.has_key("RfcpBinary"):
            args['Binary'] = mbInstance['RfcpBinary']
        if mbInstance.has_key("RfcpOptions"):
            args['Options'] = mbInstance['RfcpOptions']
        if mbInstance.has_key('RfcpTimeout'):
            args['Timeout'] = mbInstance['RfcpTimeout']
        else:
            args['Timeout'] = 5
        return args


    def _Copy(self, source, target, **args):
        """
        Build and run the rfcp command to do the transfer
        checking the exit code for success.
        A timeout is included to avoid long waits
        for kerberos
        """
        binary = args.get('Binary', 'rfcp')
        #opts   = args.get('Options', '-r')
        timeout = args['Timeout']

        comm = "%s %s %s" % (
            binary,
            source,
            target,
            )
        starttime = time.time()
        pop = popen2.Popen4(comm)
        while pop.poll() == -1:
            timenow = time.time()
            timediff = timenow-starttime
            if timediff > timeout:
                msg = "Rfcp Transport timed out after "
                msg += "%s seconds\n" % timeout
                msg +="Command Used: %s" % comm
                raise TransportFailed(
                    msg,
                    ModuleName = "MB.transport.RFIOTransporter",
                    ClassName = "RFIOTransporter",
                    MethodName = "_Copy")
        exitCode = pop.poll()
        del pop
        if exitCode:
            msg = "rfcp transport failed with exit code:"
            msg += " %s\n" % exitCode
            msg +="Command Executed: %s" % comm
            raise TransportFailed(
                msg,
                ModuleName = "MB.transport.RFIOTransporter",
                ClassName = "RFIOTransporter",
                MethodName = "_Copy")
        return
