#!/usr/bin/env python
"""
_Submitters_

Submission interfaces package

"""
__all__ = []
__version__ = "$Revision: 1.6 $"
__revision__ = "$Id: __init__.py,v 1.6 2007/02/15 20:34:00 evansde Exp $"



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

