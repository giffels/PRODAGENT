#!/usr/bin/env python
"""
_ShREEKStdoutAdapter_

ShLoggerAdapter implementation to format logger
output from ShREEK
"""

import sys

from ShLogger.ShLoggerAdapter import ShLoggerAdapter
from ShLogger.ShLoggerRegistry import registerShLoggerAdapter


class ShREEKStdoutAdapter(ShLoggerAdapter):
    """
    _ShREEKStdoutAdapter_

    Format logger messages tersely for ShREEK output
    
    """
    def __init__(self):
        ShLoggerAdapter.__init__(self)
        self._OutputHandle = sys.stdout


    def initAdapter(self, **args):
        """
        _initAdapter_

        Initialise this adapter.
        """
        if args.get("Output", False):
            self._OutputHandle = args['Output']

            

    def handleMessage(self, **args):
        """
        _handleMessage_

        Write Messages to output handle
        """
        msg = "==>%s From: %s\n" % (args["level"], args["object"])
        msg += "==>Message:\n"
        msg += args["message"]
        msg += "\n"
        msg += "==>End\n"
        self._OutputHandle.write(msg)
        return
    
registerShLoggerAdapter(ShREEKStdoutAdapter, "stdout")
