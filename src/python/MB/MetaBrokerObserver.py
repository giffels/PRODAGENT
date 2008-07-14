#!/usr/bin/env python
"""
_MetaBrokerObserver_

Module containing class definition for a MetaBrokerObserver.
A MetaBrokerObserver is an object that can be Registered
with a MetaBroker object in order to recieve update calls
when the state of the observee changes

"""
__revision__ = \
     "$Id: MetaBrokerObserver.py,v 1.1 2005/12/30 18:51:37 evansde Exp $"
__version__ = "$Revision: 1.1 $"

from MB.MetaBroker import MetaBroker
from MB.MBException import MBException

class MetaBrokerObserver:
    """
    _MetaBrokerObserver_

    Object to monitor a MetaBroker instance
    and register itself as an observer with the
    MB instance being observerd. This means that
    this object will be updated automatically when
    any change occurs in the MB being watched
    """
    def __init__(self, watchedMetaBroker = None):
        self._Watched = None
        if watchedMetaBroker != None:
            self.setWatched(watchedMetaBroker)
        

    def getWatched(self):
        """return the reference to the MB being watched"""
        return self._Watched

    def setWatched(self, newWatchedMB):
        """
        _setWatched_

        Set the reference to the MetaBroker to be observed
        """
        if not isinstance(newWatchedMB, MetaBroker):
            msg = "Non MetaBroker Object passed to "
            msg += "MetaBrokerObserver"
            raise MBException(
                msg, ModuleName = "MB.MetaBrokerObserver",
                ClassName = "MetaBrokerObserver",
                MethodName = "setWatched",
                BadObject = newWatchedMB,
                )
        self._Watched = newWatchedMB
        self._Watched.registerObserver(self)
        return

    def updateObserver(self):
        """
        Override to define the action taken when updated
        """
        pass

    
        

