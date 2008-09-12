# $Id: LogStates.py,v 1.1 2005/12/30 18:57:45 evansde Exp $ 
# pylint: disable-msg=E0211
# Disable complaint about _GenerateTimestamp not requiring an arg
"""
Module containing Log State definitions and utils for handling them easily.

Initial definitions are simple strings that can be used for formatting
output. The content of the strings is sufficiently weird to avoid duplicate
symbols.

The LogStates class is used as a namespace to contain all of the Log States
and associated objects/methods
"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: LogStates.py,v 1.1 2005/12/30 18:57:45 evansde Exp $"

from time import time, localtime, asctime


class LogStates:
    """
    Namespace to contain all of the Log States and associated
    objects/methods
    """
    def __init__(self):
        msg = "LogStates Object is a namespace and should"
        msg += "not be instantiated"
        raise RuntimeError, msg
    
    __ClassName__LogStates__ = None
    #  //
    # // Error Level
    #//
    Error = "ShLogger:_**ERROR**_:"

    #  //
    # // Alert Level
    #//
    Alert = "ShLogger:__*Alert*__:"

    #  //
    # // Informational Level
    #//
    Info  = "ShLogger:___Info____:"
   
    #  //
    # // Debug levels lo, med, hi
    #//
    Dbg_lo  = "ShLogger:_Debug_low__:"
    Dbg_med = "ShLogger:_Debug_med_:"
    Dbg_hi  = "ShLogger:_Debug_high__:"
    
    #  //
    # // Lists for easy membership checks
    #//
    #  //
    # // All levels in hierarchical order of severity
    #//
    Levels = [ Error, Alert, Info, Dbg_lo, Dbg_med,Dbg_hi]

    #  //
    # // All debug level flags
    #//
    DbgLevels = [ Dbg_lo, Dbg_med, Dbg_hi]

    #  //
    # // Generate a timestamp string. The localtime converts the
    #//  Unix Epoch time() into the local time zone value
    #  //and asctime converts this into a nice string format.
    # // 
    #//
    def _GenerateTimestamp():
        """
        Generate a timestamp string. The localtime converts the
        Unix Epoch time() into the local time zone value
        and asctime converts this into a nice string format.
        """
        return '%s' % asctime(localtime(time()))

    generateTimestamp = staticmethod(_GenerateTimestamp)
    

#  //
# //
#//------------end class LogStates--------------------------
