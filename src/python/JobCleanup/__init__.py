#!/user/bin/env python

"""
_JobCleanup_

Component that handles cleanup events.
The pluggable structure allows for different
types of cleanup events. Currently there
are the JobCleanup and the PartialJobCleanup.
The first removes all job information from the
prodagent while the latter just compacts the 
files in the cache dir.

"""
__revision__ = "$Id: __init__.py,v 1.1 2006/05/19 05:50:20 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "fvlingen@caltech.edu"

import JobCleanup.Handlers
