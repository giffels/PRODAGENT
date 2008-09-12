# $Id: LogInterface.py,v 1.1 2005/12/30 18:57:45 evansde Exp $ 
"""
Log interface module.
Interface to Logging system for all objects. 
Any object that needs to talk to the logger should inherit this class.

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: LogInterface.py,v 1.1 2005/12/30 18:57:45 evansde Exp $"



from ShLogger.LogStates import LogStates
from ShLogger.LogMgr import getLogMgr



class LogInterface:
    """
    Logging interface class.

    Provides a log call that passes methods to the LogManager
    """
    def __init__(self, objectName = None):
        self.__ClassName__LogInterface__ = None
        self._LOG_ObjectName = (objectName or self.__class__.__name__)
        self._LOG_LogMgrRef  = None
        self._VetoLevels = []

    def logMgrRef(self):
        """
        Reference to the LogMgr singleton instance
        starts as None, to avoid deepcopy problems.
        Is automatically looked up when referenced.
        """
        if self._LOG_LogMgrRef == None:
            self._LOG_LogMgrRef = getLogMgr()
        return self._LOG_LogMgrRef
    

    def __deepcopy__(self, memo):
        """
        _deepcopy_
        
        Implemented to stop recursive copying of LogMgr Reference.
        If an object with a LogInterface is copied
        
        """
        liCopy = LogInterface()
        liCopy.__ClassName__LogInterface__ = self.__ClassName__LogInterface__
        liCopy._LOG_ObjectName = self._LOG_ObjectName 
        return liCopy




    def log(self, message, level):
        """
        _log_

        Main Logging Method used to handle log messages.

        Args --

        - *message* :  descriptive string message
        
        - *level* :    Log Level of the message. Must be a valid LogState level

        """
        #  //
        # // If invalid level or vetoed level, dump the message 
        #//
        if level not in LogStates.Levels:
            return
        if self._LOGFilterMessage(level):
            return
        #  //
        # // If we are keeping this message, then dispatch it
        #//  to the LogMgr, adding the timestamp and object name
        self.logMgrRef().dispatchMessage(
            message = message, 
            level = level, 
            timestamp = LogStates.generateTimestamp(), 
            object = self._LOG_ObjectName
            )
        return

    def addLogVeto(self, level):
        """
        _addLogVeto_
        
        Add a log level to the list of Vetoed Levels
        level must be one of the valid level symbols

        Args --
        
        - *level* : Log Level to be silenced
        
        """
        if level not in LogStates.Levels:
            return
        if level not in self.logMgrRef()._LOG_VetoLevels:
            self.logMgrRef()._LOG_VetoLevels.append(level)
        return

    def addLocalLogVeto(self, level):
        """
        _addLocalLogVeto_

        Add a veto'd log level to the local Veto List so
        that it is veto'd by this instance and not globally
        by the LogMgr
        
        Args --
        
        - *level* : Log Level to be silenced
        
        """
        if level not in LogStates.Levels:
            return
        if level not in self._VetoLevels:
            self._VetoLevels.append(level)
        return
        
    def clearLogVeto(self, level = None):
        """
        _clearLogVeto_
        
        Clear veto method, if the level is given, that level is removed 
        from the _VetoList, if no level is given then all levels are
        removed so the output gets 'Loud'.

        Args --

        - *level* : Log Level to clear veto for. If not specified or None,
        this will clear all log vetos
        
        """
        if level == None:
            self.logMgrRef()._LOG_VetoLevels = []
            return
        while level in self.logMgrRef()._LOG_VetoLevels:
            self.logMgrRef()._LOG_VetoLevels.remove(level)

        return

    def clearLocalLogVeto(self, level):
        """
        _clearLocalLogVeto_

        Clear a local veto of a log level maintained within this
        instance.

        Args --

        - *level* : Log Level to clear veto for. If not specified or None,
        this will clear all log vetos
        
        """
        if level == None:
            self._VetoLevels = []
            return
        while level in self._VetoLevels:
            self._VetoLevels.remove(level)
        return

    def logSilence(self):
        """
        Turn off all logging output globally
        """
        self.logMgrRef()._LOG_VetoLevels= LogStates.Levels[:]
        return


    def localLogSilence(self):
        """
        Turn off all logging output for this Object only
        """
        self._VetoLevels = LogStates.Levels[:]
        return
        
        

    def _LOGFilterMessage(self, level):
        """
        Filter condition. If the level of the message is
        in the _LOG_VetoLevels list, return 1
        else return 0. 1 means that the message will be dropped
        """
        if level in self.logMgrRef()._LOG_VetoLevels:
            return 1
        if level in self._VetoLevels:
            return 1
        return 0



