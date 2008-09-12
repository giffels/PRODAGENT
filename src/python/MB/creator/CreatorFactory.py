#!/usr/bin/env python
"""
CreatorFactory

Module defining the transport method manager

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: CreatorFactory.py,v 1.1 2005/12/30 18:51:39 evansde Exp $"
__author__ = "evansde@fnal.gov"



from MB.MBException import MBException
from MB.MBException import FactorySingleton

from MB.creator.Creator    import Creator

from MB.creator.LocalCreator import LocalCreator
from MB.creator.RSHCreator import RSHCreator
from MB.creator.SSHCreator import SSHCreator
from MB.creator.GlobusJobRunCreator import GlobusJobRunCreator
from MB.creator.RFIOCreator import RFIOCreator

#  //
# // Accessor to return factory instance
#//
def getCreatorFactory():
    """
    Returns singleton CreatorFactory object
    """
    single = None
    try:
        return CreatorFactory()
    except FactorySingleton, singleton:
        single = singleton.instance()
    return single


class CreatorFactory(dict):
    """
    _CreatorFactory_

    Dictionary based singleton factory for mapping
    creator methods to creator handlers
    """
    __singleton = None

    def __init__(self):
        if ( self.__singleton is not None ):
            instance = CreatorFactory.__singleton
            raise FactorySingleton(instance)
        CreatorFactory.__singleton = self
        dict.__init__(self)
        
        self.registerCreatorMethod("local", LocalCreator())
        self.registerCreatorMethod("rsh", RSHCreator())
        self.registerCreatorMethod("ssh", SSHCreator())
        self.registerCreatorMethod("globus", GlobusJobRunCreator())
        self.registerCreatorMethod("dcap", LocalCreator())
        self.registerCreatorMethod("rfio", RFIOCreator())
        
        
    def __setitem__(self, key, value):
        self.registerCreatorMethod(key, value)
        return

    

    def registerCreatorMethod(self, name, handler):
        """
        _registerCreatorMethod_

        Register a creator method for a particular
        creator protocol

        Args --

        - *name* : Name of the protocol (eg rcp, ftp etc)

        - *handler* : Specialised Creator instance for
        handling the specified protocol
        
        """
        if not isinstance(handler, Creator):
            msg = "Creator Factopy Error: "
            msg += "Non Creatorer instance registered "
            msg += "with CreatorFactory"
            raise MBException(
                msg,
                ClassInstance = self,
                Handler = handler,
                HandlerName = name)
        dict.__setitem__(self, name, handler)
        return
