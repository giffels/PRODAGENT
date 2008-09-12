#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_SCPTransporter_

Transporter implementation for using scp

"""
__version__ = "$Version$"
__revision__ = "$Id: SCPTransporter.py,v 1.1 2005/12/30 18:51:41 evansde Exp $"
 

import popen2
import time

from MB.transport.Transporter import Transporter
from MB.transport.TransportException import TransportFailed

class SCPTransporter(Transporter):
    """
    _SCPTransporter_

    Specialisation of Transporter for handling transfers
    via scp
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
        mbInstance['URL'] = "scp://%s" % mbInstance['RemoteName']
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
        mbInstance['URL'] = "scp://%s" % mbInstance['Target']
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
        mbInstance['URL'] = "scp://%s" % mbInstance['Target']
        return

    

    def _ExtractOpts(self, mbInstance):
        """
        Extract any extra options for SCP Transfer
        from the metabroker instance and construct the
        args dictionary
        """
        args = {}
        if mbInstance.has_key("ScpBinary"):
            args['Binary'] = mbInstance['ScpBinary']
        if mbInstance.has_key("ScpOptions"):
            args['Options'] = mbInstance['ScpOptions']
        if mbInstance.has_key('ScpTimeout'):
            args['Timeout'] = mbInstance['ScpTimeout']
        else:
            args['Timeout'] = 5
        return args


    def _Copy(self, source, target, **args):
        """
        Build and run the scp command to do the transfer
        checking the exit code for success.
        A timeout is included to avoid long waits
        for kerberos
        """
        binary = args.get('Binary', 'scp')
        opts   = args.get('Options', '-r')
        timeout = args['Timeout']

        comm = "%s %s %s %s" % (
            binary,
            opts,
            source,
            target,
            )
        starttime = time.time()
        pop = popen2.Popen4(comm)
        while pop.poll() == -1:
            timenow = time.time()
            timediff = timenow-starttime
            if timediff > timeout:
                msg = "Scp Transport timed out after "
                msg += "%s seconds\n" % timeout
                msg +="Command Used: %s" % comm
                raise TransportFailed(
                    msg,
                    ModuleName = "MB.transport.SCPTransporter",
                    ClassName = "SCPTransporter",
                    MethodName = "_Copy")
        exitCode = pop.poll()
        del pop
        if exitCode:
            msg = "rcp transport failed with exit code:"
            msg += " %s\n" % exitCode
            msg +="Command Executed: %s" % comm
            raise TransportFailed(
                msg,
                ModuleName = "MB.transport.SCPTransporter",
                ClassName = "SCPTransporter",
                MethodName = "_Copy")
        return
