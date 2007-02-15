#!/usr/bin/env python
"""
_Submitters_

Submission interfaces package

"""
__all__ = []
__version__ = "$Revision: 1.5 $"
__revision__ = "$Id: __init__.py,v 1.5 2006/09/28 12:58:27 bacchi Exp $"



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
