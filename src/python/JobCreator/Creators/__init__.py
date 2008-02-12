#!/usr/bin/env python
"""
_JobCreator.Creators_

Plugins and tools for creating Jobs and acting on TaskObjects

"""
__version__ = "$Revision: 1.9 $"
__revision__ = "$Id: __init__.py,v 1.9 2007/12/18 22:25:56 evansde Exp $"
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
import JobEmulatorCreator
