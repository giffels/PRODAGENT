#!/usr/bin/env python
"""
_Create_

Creators interface that maps a MetaBroker instance to the
appropriate Creator to create files or dirs based on the target values.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Version$"

from MB.creator.CreatorFactory import getCreatorFactory

_CreatorFactory = getCreatorFactory()



def createTarget(mbInstance):
    """
    _createTarget_

    Based on the Type and target values contained in the
    metabroker instance provided, an empty file or new directory
    will be constructed on the target host based on the method specified
    in the CreateMethod key of the MetaBroker.

    If creation fails, a CreatorException will be thrown, (this can be caught
    as either a CreatorException or an MBException)
    

    """
    createMethod = _CreatorFactory[mbInstance['CreatorMethod'] ]
    result = createMethod(mbInstance)
    
    return result
    
