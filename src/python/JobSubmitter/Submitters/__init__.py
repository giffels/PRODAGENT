#!/usr/bin/env python
"""
_Submitters_

Submission interfaces package

"""
__all__ = []
__version__ = "$Revision: 1.21 $"
__revision__ = "$Id: __init__.py,v 1.21 2008/10/31 15:38:25 gutsche Exp $"



#  //
# // Import all submitter impl modules here to register them on import
#//
import NoSubmit
import CondorSubmitter
import CondorGSubmitter
import OSGSubmitter
import OSGBulkSubmitter
import OSGResConBulkSubmitter
import OSGRouter
import OSGBulkRouter
import T0LSFSubmitter
import OSGGlideIn
import CondorDirect
import ARCSubmitter
import JobEmulatorBulkSubmitter
import GlideInWMS
import FNALLPCCAF
import BlGLiteBulkSubmitter
import BlGLiteBulkResConSubmitter

