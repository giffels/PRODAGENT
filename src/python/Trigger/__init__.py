#!/usr/bin/env python
"""
_Trigger_

Module that enables components to set triggers and associate
flags to triggers. If all flags are set the associated action
if invoked, as specified in the action registery. Triggers enable
parallel execution of multiple tasks and synchronizing them by using
1 flag per tasks which is set once the task is finished.

"""
__revision__ = "$Id: __init__.py,v 1.1 2006/04/28 17:06:16 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"


import TriggerAPI 
#import Database
import Actions

