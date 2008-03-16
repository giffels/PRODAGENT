#!/usr/bin/env python
"""

Glite Collection implementation.

"""

__revision__ = "$Id: GLiteBulkSubmitter.py,v 1.24 2008/02/19 16:13:58 afanfani Exp $"
__version__ = "$Revision: 1.24 $"

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.GLiteBulkInterface import GLiteBulkInterface

class GLiteBulkSubmitter(GLiteBulkInterface):
    """
    Class to do GLite bulk submission
    """

registerSubmitter(GLiteBulkSubmitter, GLiteBulkSubmitter.__name__)