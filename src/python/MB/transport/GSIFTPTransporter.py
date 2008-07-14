#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_GSIFTPTransporter_

Transporter implementation for using globus-url-copy

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: GSIFTPTransporter.py,v 1.1 2005/12/30 18:51:41 evansde Exp $"

import popen2
import time

from MB.transport.Transporter import Transporter
from MB.transport.TransportException import TransportFailed



class GSIFTPTransporter(Transporter):
    """
    _GSIFTPTransporter_

    Transfers via globus-url-copy

    """

    def __init__(self):
        Transporter.__init__(self)





    def transportIn(self, mbInstance):
        """
        Handle Transport from Source to Current of the form:

        gsiftp:remote -> file:local

        """
        args = self._ExtractOpts(mbInstance)
        command = "%s %s " % (args["Binary"], args["GSIFTPFlags"])
        command += "gsiftp://%s/%s" % (mbInstance["SourceHostName"],
                                     mbInstance["SourceAbsName"])
        command += " file://%s " % mbInstance["AbsName"]
        try:
            self._ExecuteCopy(command)
        except TransportFailed, ex:
            ex.addInfo(MetaBroker = mbInstance)
            ex.addInfo(CallingMethod = "transportIn")
            raise ex
        mbInstance['URL'] = "file://%s " % mbInstance["AbsName"]
        return


    def transportOut(self, mbInstance):
        """
        Handle Transport from Current to Target of the form

        file:local -> gsiftp:remote

        """
        args = self._ExtractOpts(mbInstance)
        command = "%s %s " % (args["Binary"], args["GSIFTPFlags"])
        command += " file://%s " % mbInstance["AbsName"]
        command += "gsiftp://%s/%s" % (mbInstance["TargetHostName"],
                                     mbInstance["TargetAbsName"])
        
        try:
            self._ExecuteCopy(command)
        except TransportFailed, ex:
            ex.addInfo(MetaBroker = mbInstance)
            ex.addInfo(CallingMethod = "transportOut")
            raise ex
        mbInstance['URL'] = "gsiftp://%s/%s" % (mbInstance["TargetHostName"],
                                                mbInstance["TargetAbsName"])
        return
        
    def transportInOut(self, mbInstance):
        """
        Handle transport from Source to Target via local file
        system:

        gsiftp:remote1 -> file:local -> gsiftp:remote2

        """
        args = self._ExtractOpts(mbInstance)
        command = "%s %s " % (args["Binary"], args["GSIFTPFlags"])
        command += "gsiftp://%s//%s" % (mbInstance["SourceHostName"],
                                     mbInstance["SourceAbsName"])

        command += "  gsiftp://%s//%s" % (mbInstance["TargetHostName"],
                                        mbInstance["TargetAbsName"])
        
        try:
            self._ExecuteCopy(command)
        except TransportFailed, ex:
            ex.addInfo(MetaBroker = mbInstance)
            ex.addInfo(CallingMethod = "transportInOut")
            raise ex
        mbInstance['URL'] = "gsiftp://%s/%s" % (mbInstance["TargetHostName"],
                                                mbInstance["TargetAbsName"])
        
        return
        


    def _ExtractOpts(self, mbInstance):
        """
        _ExtractOpts_

        Get any optional values for the gsiftp command
        from the MetaBroker instance

        """
        args = {}
        args['Binary'] = mbInstance.get("Binary", "globus-url-copy")
        args['GSIFTPFlags'] = mbInstance.get("Options", "")
        
        return args



    def _ExecuteCopy(self, copyCommand):
        """
        _ExecuteCopy_

        Use popen to run the copy command provided
        """
        pop = popen2.Popen4(copyCommand)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()
        commOut = pop.fromchild.read()
        del pop
        if exitCode:
            msg = "gsiftp transport failed with exit code:"
            msg += " %s\n" % exitCode
            msg +="Command Executed: %s" % copyCommand
            raise TransportFailed(
                msg,
                ModuleName = "MB.transport.GSIFTPTransporter",
                ClassName = "GSIFTPTransporter",
                MethodName = "_ExecuteCopy",
                CommandOutput = commOut)
        return
        
