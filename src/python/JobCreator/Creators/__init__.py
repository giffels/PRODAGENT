#!/usr/bin/env python
"""
_JobCreator.Creators_

Plugins and tools for creating Jobs and acting on TaskObjects

"""
__version__ = "$Revision: 1.4 $"
__revision__ = "$Id: __init__.py,v 1.4 2007/01/12 23:20:31 evansde Exp $"
__all__ = []


#  //
# // import modules containing creators here to automatically register them
#//
import TestCreator
#import LXB1125Creator
import LCGCreator
import FNALCreator
#import LSFCreator
import LCGCreator
import ExampleCreator
import OSGCreator
import OSGBulkCreator

