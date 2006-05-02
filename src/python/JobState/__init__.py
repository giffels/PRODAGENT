#!/usr/bin/env python
"""
_JobState_

Component that keeps track of the job(spec) state and 
makes it persistent in a database. The job state supports the 
internal auditing capability within the prodagent. It does not 
keep track of any information regarding a running job. This is the
task of the job tracker.

"""
__revision__ = "$Id: __init__.py,v 1.1 2006/04/10 17:06:16 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"


import JobStateAPI
import Database 
