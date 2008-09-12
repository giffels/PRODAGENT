#!/usr/bin/env python
"""
TransportFactory

Module defining the transport method manager

"""

__version__ = "$Version$"
__revision__ = "$Id: TransportFactory.py,v 1.3 2006/03/08 13:13:03 afanfani Exp $"



from MB.MBException import MBException
from MB.MBException import FactorySingleton

from MB.transport.Transporter    import Transporter
from MB.transport.CPTransporter  import CPTransporter
from MB.transport.RCPTransporter import RCPTransporter
from MB.transport.SCPTransporter import SCPTransporter
from MB.transport.RFIOTransporter import RFIOTransporter
from MB.transport.DCAPTransporter import DCAPTransporter
from MB.transport.GSIFTPTransporter import GSIFTPTransporter
from MB.transport.SRMTransporter import SRMTransporter
from MB.transport.LCGTransporter import LCGTransporter
#  //
# // Accessor to return factory instance
#//
def getTransportFactory():
    """
    Returns singleton TransportFactory object
    """
    single = None
    try:
        return TransportFactory()
    except FactorySingleton, singleton:
        single = singleton.instance()
    return single


class TransportFactory(dict):
    """
    _TransportFactory_

    Dictionary based singleton factory for mapping
    transport methods to transport handlers
    """
    __singleton = None

    def __init__(self):
        if ( self.__singleton is not None ):
            instance = TransportFactory.__singleton
            raise FactorySingleton(instance)
        TransportFactory.__singleton = self
        dict.__init__(self)
        self.registerTransportMethod('cp',
                                     CPTransporter())
        self.registerTransportMethod('rcp',
                                     RCPTransporter())
        self.registerTransportMethod('scp',
                                     SCPTransporter())
        self.registerTransportMethod('rfio:',
                                      RFIOTransporter())  
        self.registerTransportMethod('dccp',
                                      DCAPTransporter())
        self.registerTransportMethod('gsiftp',
                                      GSIFTPTransporter())
        self.registerTransportMethod('srm',
                                      SRMTransporter())
        self.registerTransportMethod('lcg',
                                      LCGTransporter())

    def __setitem__(self, key, value):
        self.registerTransportMethod(key, value)
        return

    

    def registerTransportMethod(self, name, handler):
        """
        _registerTransportMethod_

        Register a transport method for a particular
        transport protocol

        Args --

        - *name* : Name of the protocol (eg rcp, ftp etc)

        - *handler* : Specialised Transporter instance for
        handling the specified protocol
        
        """
        if not isinstance(handler, Transporter):
            msg = "Transport Factopy Error: "
            msg += "Non Transporter instance registered "
            msg += "with TransportFactory"
            raise MBException(
                msg,
                ModuleName = "MB.transport.TransportFactory",
                ClassName = "TransportFactory",
                MethodName = "registerTransportMethod",
                Handler = handler,
                HandlerName = name)
        dict.__setitem__(self, name, handler)
        return
