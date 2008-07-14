#!/usr/bin/env python
"""
CommandFactory

Factory object used to register and instantiate CommandBuilder
implementations for given access protocols

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: CommandFactory.py,v 1.1 2005/12/30 18:51:38 evansde Exp $"
__author__ = "evansde@fnal.gov"



from MB.MBException import MBException
from MB.MBException import FactorySingleton
from MB.commandBuilder.CommandBuilder import CommandBuilder

#  //
# // Accessor to return factory instance
#//
def getCommandFactory():
    """
    Returns singleton CommandFactory object
    """
    single = None
    try:
        return CommandFactory()
    except FactorySingleton, singleton:
        single = singleton.instance()
    return single


class CommandFactory(dict):
    """
    _CommandFactory_

    Dictionary based singleton factory for mapping
    transport methods to transport handlers
    """
    __singleton = None

    def __init__(self):
        if ( self.__singleton is not None ):
            instance = CommandFactory.__singleton
            raise FactorySingleton(instance)
        CommandFactory.__singleton = self
        dict.__init__(self)


    def __setitem__(self, key, value):
        self.registerAccessProtocol(key, value)
        return


    def __getitem__(self, key):
        """
        implement getitem to instantiate the handler object
        for the access protocol provided by the key
        """
        return self.instantiateHandler(key)

    def instantiateHandler(self, protocol):
        """
        _instantiateHandler_

        Create and return an instance of the handler provided
        based on the protocol
        """
        handlerClass = dict.get(self, protocol, None)
        if handlerClass == None:
            msg = "No Implementation registered for protocol:\n"
            msg += "%s\n" % protocol
            raise MBException(msg, ClassInstance = self,
                              MissingProtocol = protocol)
        
        try:
            handler = handlerClass()
        except StandardError, ex:
            msg = "Error instantiating Access Protocol Handler for:\n"
            msg += "%s\n" % protocol
            msg += "Exception details:\n%s\n" % str(ex)
            raise MBException(msg, ClassInstance = self,
                              Protocol = protocol)
        return handler
        
    def registerAccessProtocol(self, name, handlerClass):
        """
        _registerTransportMethod_

        Register a transport method for a particular
        transport protocol

        Args --

        - *name* : Name of the access protocol (eg rcp, ftp etc)

        - *handler* : Class Reference to the handler to be used
        
        """
        if not issubclass(handlerClass, CommandBuilder):
            msg = "CommandFactory Error: "
            msg += "Non CommandBuilder implementation registered "
            msg += "with CommandFactory"
            raise MBException(
                msg,
                ClassInstance = self,
                Handler = handlerClass,
                Protocol = name)
        dict.__setitem__(self, name, handlerClass)
        return
