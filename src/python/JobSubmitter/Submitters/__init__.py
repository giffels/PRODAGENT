#!/usr/bin/env python
"""
_Submitters_

Submission interfaces package

"""
__all__ = []
__version__ = "$Revision: 1.3 $"
__revision__ = "$Id: __init__.py,v 1.3 2006/06/13 10:22:32 bacchi Exp $"



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
import BOSSCondorGSubmitter
import OSGRouter
