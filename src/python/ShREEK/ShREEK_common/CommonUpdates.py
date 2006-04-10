# pylint: disable-msg=W0613
#
# Disable pylint warning about unused args for updateTime and 
# updateWorkingDir methods
"""
Process state update module.
"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: CommonUpdates.py,v 1.1 2005/12/30 18:54:28 evansde Exp $"

import os
import re

from time import time, asctime, localtime

from ShREEK.ShREEKPluginMgr import registerShREEKUpdator




def updateTime(state):
    """
    Time Stamp Generator.
    """
    return asctime(localtime(time()))


def updateWorkingDir(state):
    """
    Directory Update method.
    """
    return os.getcwd()





                               
registerShREEKUpdator(updateTime, "Time")
registerShREEKUpdator(updateWorkingDir, "WorkingDir")

