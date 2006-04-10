#!/usr/bin/env python
"""
_Submitters_

Submission interfaces package

"""
__all__ = []
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: __init__.py,v 1.1 2006/03/20 22:37:04 evansde Exp $"



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

