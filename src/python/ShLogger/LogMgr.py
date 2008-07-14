# pylint: disable-msg=W0152,W0611
# Disable check on bad ** magic, unused imports
"""
Log manager module. defines and maintains ShLogger LogMgr singleton instance
"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: LogMgr.py,v 1.1 2005/12/30 18:57:45 evansde Exp $"

import os
import socket

from ShLogger.LogStates import LogStates
from ShLogger.ShLoggerException import ShLoggerException
from ShLogger.ShLoggerRegistry import loadShLoggerAdapter

import ShLogger.log_adapters

def getLogMgr():
    """
    Returns singleton LogMgr object
    """
    single = None
    try:
        return LogMgr()
    except LogMgr, sobj:
        single = sobj
    return single



class LogMgr:
    """
    _LogMgr_
    
    LogMgr class to recieve and distribute log messages to various handlers.
    All Access to this class should be via the getLogMgr() method above
    """
    __singleton = None


    def __init__(self):
        self.__ClassName__LogMgr__ = None
        if ( self.__singleton is not None ):
            raise LogMgr.__singleton
        LogMgr.__singleton = self
        
        #  //
        # // list of adapter objects
        #//
        self._Adapters = []
       
        #  //
        # // _LOG_VetoLevels is the list of Log Levels used to drop message
        #//  if a level is in this list then the message won't be sent on
        #  //Default is to veto the debug levels and allow the 
        # // info/alert/warning levels
        #//
        self._LOG_VetoLevels = [ LogStates.Dbg_lo, 
                                 LogStates.Dbg_hi, 
                                 LogStates.Dbg_med, ]
        


        #  //
        # // Get host and user info
        #//

        self._UserName = '%s' % os.environ.get('USER', os.getuid())
        self._HostName = socket.gethostbyaddr(socket.gethostname())[0]
        self._HostIP   = socket.gethostbyname(socket.gethostname())

        #  //
        # // Job id can be set separately
        #//  via an accessor 
        self._JobId    = "None"

        #  // Stdout adapter by default, even though it
        # //  stinks.
        #//
        self.attachAdapter('stdout')
        

    #  //
    # // Set the job id for the logger
    #//
    def setJobID(self, idx):
        """
        Set the job id for the logger.
        """
        self._JobId = '%s' % idx


    def logSilence(self):
        """
        _logSilence_

        Dettach all adapters so that logging output does not occur.
        Useful for removing the default stdout adapter
        added in the ctor.
        Also provides the ability to reset the LogMgr Object
        """
        self._Adapters = []
        return

    
          
    def attachAdapter(self, adapterType, **args):
        """
        _attachAdapter_
        
        Attach a new adapter instance to the LogMgr
        Args --

        - *adapterType* : The type of adapter instance
        
        """
        try:
            adapter = loadShLoggerAdapter(adapterType)
        except ShLoggerException, ex:
            ex.message += "LogMgr: Error Loading Adapter of type: %s\n" % (
                adapterType,
                )
            ex.addInfo(AdapterArguments = args)
            raise ex
        adapter.initAdapter(**args)
        self._Adapters.append(adapter)
        return


 
    def dispatchMessage(self, **args):
        """
        _dispatchMessage_

        Recieve and distribute a message to each adapter.
        
        """
        self._CheckMessage(args)

        for adapter in self._Adapters:
            adapter.handleMessage(**args)
        return

  
    def _CheckMessage(self, args):
        """
        _CheckMessage_
        
        Ensure all fields are provided in the message before
        distribution, if they are not, add a default include extra info
        from the LogMgr such as user and host info.
        """
        args.setdefault('message', 'Message Lost')
        args.setdefault('level', LogStates.Info)
        args.setdefault('object', 'Unknown Object')
        args.setdefault('timestamp', LogStates.generateTimestamp())
        args.setdefault('userid', self._UserName)
        args.setdefault('hostname', self._HostName)
        args.setdefault('hostip', self._HostIP)
        args.setdefault('jobid', self._JobId)
        return args

