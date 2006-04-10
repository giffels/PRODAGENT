#!/usr/bin/env python
# pylint: disable-msg=W0152
# disable bad ** magic
"""
_TextAdapter_


ShLoggerAdapter implementation to format messages
and send them to a file
"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: TextAdapter.py,v 1.1 2005/12/30 18:57:47 evansde Exp $"
__author__ = "evansde@fnal.gov"

import os
import socket
from time import asctime, time, localtime

from ShLogger.ShLoggerAdapter import ShLoggerAdapter
from ShLogger.ShLoggerRegistry import registerShLoggerAdapter

class TextAdapter(ShLoggerAdapter):
    """
    _TextAdapter_

    Write Log Messages to a Text File
    """
    def __init__(self):
        ShLoggerAdapter.__init__(self)
        self._Args = {}
        self._Args.setdefault("Mode", "a")
        self._Args.setdefault("Logfile", "./logfile.txt")

    def initAdapter(self, **args):
        """
        _initAdapter_

        Initialise Adapter by creating the logfile required
        and writing an opening message
        """
        self._Args.update(args)
        
        msgDict = {}
        msgDict['object'] = "Text Adapter Starting"
        msgDict['timestamp'] = '%s' % asctime(localtime(time()))
        msgDict['level'] = "init"
        msgDict['message'] = 'Linker process %d opening logfile' % os.getpid()
        msgDict['userid'] = '%s' % os.environ.get('USER', os.getuid())
        msgDict['hostname'] = socket.gethostbyaddr(socket.gethostname())[0]
        self.handleMessage(**msgDict)


    def handleMessage(self, **args):
        """
        _handleMessage_

        Write the message to the file
        """
        fdp = open(self._Args['Logfile'], self._Args['Mode'])
        fdp.write('----Log Message----\n')
        fdp.write(args['level']+'\n')
        fdp.write('Object : %s\n' % args['object'])
        fdp.write('Time   : %s\n' % args['timestamp'])
        fdp.write('Message: %s\n' % args['message'])
        fdp.write('User   : %s\n' % args['userid'])
        fdp.write('Host   : %s\n' % args['hostname'])
        fdp.write('----End Message-------\n')
        fdp.close()
        return
registerShLoggerAdapter(TextAdapter, 'text')
