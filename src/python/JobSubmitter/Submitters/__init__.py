#!/usr/bin/env python
"""
_Submitters_

Submission interfaces package

"""
__all__ = []
__version__ = "$Revision: 1.11 $"
__revision__ = "$Id: __init__.py,v 1.11 2007/06/22 12:14:47 evansde Exp $"



#  //
# // Import all submitter impl modules here to register them on import
#//
import NoSubmit
import BOSSSubmitter
import CondorSubmitter
import CondorGSubmitter
import LXB1125Submitter
import LSFSubmitter
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
import RESubmitter
import T0LSFSubmitter
import OSGGlideIn
