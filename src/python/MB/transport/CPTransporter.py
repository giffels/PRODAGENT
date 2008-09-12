#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_CPTransporter_

Transporter Implementation for using a simple cp

"""
__version__ = "$Version$"
__revision__ =  "$Id: CPTransporter.py,v 1.1 2005/12/30 18:51:41 evansde Exp $"
 

import popen2

from MB.transport.Transporter import Transporter
from MB.transport.TransportException import TransportFailed


class CPTransporter(Transporter):
    """
    _CPTransporter_

    Specialisation of Transporter class to transport
    objects using cp based commands
    """
    def __init__(self):
        Transporter.__init__(self)


    def transportIn(self, mbInstance):
        """
        Handle Transports from Source to Memory Values
        """
        source = mbInstance['SourceAbsName']
        target = mbInstance['AbsName']
        if (source == None) or (target == None):
            return
    
        args = self._ExtractOpts(mbInstance)
        try:
            self._Copy(source, target, **args)
        except TransportFailed,ex:
            ex.addInfo(MetaBroker = mbInstance)
            raise ex
        mbInstance['URL'] = "file://%s" % mbInstance['AbsName']
        return
        
    def transportOut(self, mbInstance):
        """
        Handle Transports from Memory to Target Values
        """
        source = mbInstance['AbsName']
        target = mbInstance['TargetAbsName']
        if (source == None) or (target == None):
            return
    
        args = self._ExtractOpts(mbInstance)
        try:
            self._Copy(source, target, **args)
        except TransportFailed,ex:
            ex.addInfo(MetaBroker =  mbInstance)
            raise ex
        mbInstance['URL'] = "file://%s" % mbInstance['AbsName']
        return

    def transportInOut(self, mbInstance):
        """
        Handle transports from Source to Target Values
        """
        source = mbInstance['SourceAbsName']
        target = mbInstance['TargetAbsName']
        if (source == None) or (target == None):
            return
    
        args = self._ExtractOpts(mbInstance)
        try:
            self._Copy(source, target, **args)
        except TransportFailed,ex:
            ex.addInfo(MetaBroker = mbInstance)
            raise ex
        mbInstance['URL'] = "file://%s" % mbInstance['AbsName']
        return


    def _ExtractOpts(self, mbInstance):
        """
        Extract extra transport options from the
        metabroker if it has them
        """
        args = {}
        if mbInstance.has_key("CpBinary"):
            args['Binary'] = mbInstance['RcpBinary']
        if mbInstance.has_key("CpOptions"):
            args['Options'] = mbInstance['CpOptions']
        return args
    
    def _Copy(self, source, target, **args):
        """
        Construct the copy command and run it
        testing the exit code for sucess
        """
        binary = "cp"
        if args.has_key("Binary"):
            binary = args['Binary']

        opts = "-rf"
        if args.has_key("Options"):
            opts = args['Options']

        comm = "%s %s %s %s" % (
            binary,
            opts,
            source,
            target,
            )
        pop = popen2.Popen4(comm)
        while pop.poll() == -1:
            pass
        exitCode = pop.poll()
        del pop
        if exitCode:
            msg = "cp transport failed with exit code:"
            msg += " %s\n" % exitCode
            msg +="Command Executed: %s" % comm
            raise TransportFailed(
                msg,
                ModuleName = "MB.transport.CPTransporter",
                ClassName = "CPTransporter",
                MethodName = "_Copy")
        return
        

        
        
    
    
