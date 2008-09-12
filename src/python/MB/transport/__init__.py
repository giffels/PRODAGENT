#!/usr/bin/env python
"""
Init module for MB.transport subpackage
"""
__revision__ = "$Id: __init__.py,v 1.1 2005/12/30 18:51:41 evansde Exp $"
__version__ = "$Revision: 1.1 $"

from MB.transport.TransportFactory import getTransportFactory

transportMaker = getTransportFactory()



def getTransport(mbInstance):
    """
    _getTransport_

    Lookup the transport method for an MetaBroker instance.
    Factory method that loads the appropriate transporter
    Object and initialises it with the MetaBroker object and 
    returns it so that its transport command can be invoked.
    """
    return transportMaker[mbInstance]
    
