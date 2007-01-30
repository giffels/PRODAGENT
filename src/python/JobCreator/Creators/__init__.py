#!/usr/bin/env python
"""
_JobCreator.Creators_

Plugins and tools for creating Jobs and acting on TaskObjects

"""
__version__ = "$Revision: 1.5 $"
__revision__ = "$Id: __init__.py,v 1.5 2007/01/28 17:48:45 afanfani Exp $"
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

