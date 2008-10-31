#!/usr/bin/env python
"""
_Submitters_

Submission interfaces package

"""
__all__ = []
__version__ = "$Revision: 1.20 $"
__revision__ = "$Id: __init__.py,v 1.20 2008/07/25 15:29:17 swakef Exp $"



#  //
# // Import all submitter impl modules here to register them on import
#//
import NoSubmit
import BOSSSubmitter
import CondorSubmitter
import CondorGSubmitter
import LCGSubmitter
import LCGAdvanced
import OSGSubmitter
import OSGBulkSubmitter
import OSGResConBulkSubmitter
import BOSSCondorGSubmitter
import OSGRouter
import OSGBulkRouter
import GLITESubmitter
import GLiteBulkSubmitter
import GLiteBulkResConSubmitter
import RESubmitter
import T0LSFSubmitter
import OSGGlideIn
import CondorDirect
import ARCSubmitter
import JobEmulatorBulkSubmitter
import GlideInWMS
import FNALLPCCAF
import BlGLiteBulkSubmitter
import BlGLiteBulkResConSubmitter

