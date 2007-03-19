#!/usr/bin/env python
"""
_JobCreator.Creators_

Plugins and tools for creating Jobs and acting on TaskObjects

"""
__version__ = "$Revision: 1.6 $"
__revision__ = "$Id: __init__.py,v 1.6 2007/01/30 15:59:45 evansde Exp $"
__all__ = []


#  //
# // import modules containing creators here to automatically register them
#//
import TestCreator
#import LXB1125Creator
import FNALCreator
#import LSFCreator
import LCGCreator
import LCGBulkCreator
import ExampleCreator
import OSGCreator
import OSGBulkCreator
import T0LSFCreator
