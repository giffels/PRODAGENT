#!/usr/bin/env python
"""
_TargetURL_

Function used to generate the target URL of an FMB using the commandBuilder
targetURL function

"""

import popen2
from MB.commandBuilder.CommandFactory import getCommandFactory

_CommandFactory = getCommandFactory()



def transportTargetURL(mbInstance):
    """
    _targetURL_

    Invoke the commandBuilder for the TransportMethod of the MB instance
    to create a target URL for the metabroker

    """
    
    transportMethod = mbInstance['TransportMethod']
    commandMaker = _CommandFactory[transportMethod]
    targetURL = commandMaker.targetURL(mbInstance)
    return targetURL
