#!/usr/bin/env python
"""
_StdoutAdapter_


ShLoggerAdapter implementation to format messages
and send them to standard out.
"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: StdoutAdapter.py,v 1.1 2005/12/30 18:57:47 evansde Exp $"
__author__ = "evansde@fnal.gov"

import sys

from ShLogger.ShLoggerAdapter import ShLoggerAdapter
from ShLogger.ShLoggerRegistry import registerShLoggerAdapter

class StdoutAdapter(ShLoggerAdapter):
    """
    _StdoutAdapter_

    Handle messages by writing them to sys.stdout

    """
    def __init__(self):
        ShLoggerAdapter.__init__(self)


    def handleMessage(self, **args):
        """
        _handleMessage_

        Format the message and write it to sys.stdout
        """
        msg = '----Stdout Message----\n'
        msg += '  %s  \n' % args['level']
        msg += 'Object : %s\n' % args['object']
        msg += 'Time   : %s\n' % args['timestamp']
        msg += 'Message: %s\n' % args['message']
        msg += 'User   : %s\n' % args['userid']
        msg += 'Host   : %s\n' % args['hostname']
        msg += '----End Message-------\n'
        sys.stdout.write(msg)
        return
    
registerShLoggerAdapter(StdoutAdapter, 'stdout')
