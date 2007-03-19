#!/usr/bin/env python
"""
_Submitters_

Submission interfaces package

"""
__all__ = []
__version__ = "$Revision: 1.7 $"
__revision__ = "$Id: __init__.py,v 1.7 2007/03/05 16:48:30 bacchi Exp $"



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
import OSGSubmitter
import OSGBulkSubmitter
import BOSSCondorGSubmitter
import OSGRouter
import GLITESubmitter
import RESubmitter
import T0LSFSubmitter
import OSGGlideIn
