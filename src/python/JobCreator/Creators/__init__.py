#!/usr/bin/env python
"""
_JobCreator.Creators_

Plugins and tools for creating Jobs and acting on TaskObjects

"""
__version__ = "$Revision: 1.8 $"
__revision__ = "$Id: __init__.py,v 1.8 2007/12/04 16:05:30 evansde Exp $"
__all__ = []


#  //
# // import modules containing creators here to automatically register them
#//
import TestCreator
import LCGCreator
import LCGBulkCreator
import ExampleCreator
import OSGCreator
import OSGBulkCreator
import T0LSFCreator
import ARCCreator

