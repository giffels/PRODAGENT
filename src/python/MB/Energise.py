#!/usr/bin/env python
"""
_Energise_

Energise method for MetaBroker Objects, to initialize the Transport

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Version$"

from MB.transport.TransportFactory import getTransportFactory

_TransportFactory = getTransportFactory()

def energise(mbInstance):
    """
    _Energise_

    Straightforward initilizing 
    a MetaBroker object using the
    builtin Energise field 
    for performing the transport. 

    """
    
    transportMethod = _TransportFactory[mbInstance['TransportMethod'] ]
    result = transportMethod(mbInstance)
    
    return result
    
    


