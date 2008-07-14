#!/usr/bin/env python
"""
_ShLoggerAdapter_

Base Class for ShLogger Plugins to handle Log Messages

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ShLoggerAdapter.py,v 1.1 2005/12/30 18:57:45 evansde Exp $"
__author__ = "evansde@fnal.gov"

class ShLoggerAdapter:
    """
    _ShLoggerAdapter_

    Base Class for ShLogger Adapter Plugins
    Can be overridden to handle messages from
    LogInterface Objects.

    """
    def __init__(self):
        """
        Constructor should take no args, as adapters
        are instantiated from a factory
        """
        self._AdapterType = self.__class__.__name__

        
    def initAdapter(self, **args):
        """
        _initAdapter_

        Override to initialise the Adapter with
        a dictionary of arguments

        """
        pass

     
    def handleMessage(self, **args):
        """
        _handleMessage_
        
        Override to handle a message. The message is a
        dictionary of key value pairs. Some common
        fields will always be provided:

        message
        level
        object
        timestamp
        userid
        hostname
        hostip
        jobid
        
        """
        pass


    def shutdownLogger(self):
        """
        _shutdownLogger_

        Hook method to close down adapter
        """
        pass
    
