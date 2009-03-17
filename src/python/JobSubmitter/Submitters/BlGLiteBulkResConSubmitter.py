#!/usr/bin/env python
"""
python

Glite Collection class

"""

__revision__ = "$Id: BlGLiteBulkResConSubmitter.py,v 1.3 2009/01/23 12:58:11 gcodispo Exp $"
__version__ = "$Revision: 1.3 $"

import logging

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BlGLiteBulkSubmitter import BlGLiteBulkSubmitter

from ProdAgent.ResourceControl.ResourceControlAPI import createCEMap
                                       
def getCEstrings(Whitelist):
    """
    Get list of CE's from resource control
    """
    result = set()
    # upto ResourceMonitor to take account of site status (not submitter)
    cemap = createCEMap(activeOnly=False)
    for i in Whitelist:
        try:
            name = cemap[int(i)]
            result.add(name)
            logging.debug("Whitelist element %s" % name)
        except KeyError, ex:
            raise RuntimeError("Error mapping site id %s to ce: %s" % (str(i), str(ex)))
    return list(result)
                            


class BlGLiteBulkResConSubmitter(BlGLiteBulkSubmitter):
    """

    Base class for GLITE bulk submission should not be used
    directly but one of its inherited classes.
      
    """

    scheduler  = "SchedulerGLiteAPI"

    def __init__(self):
        #super(BlGLiteBulkSubmitter, self).__init__()
        BlGLiteBulkSubmitter.__init__(self)
        self.whitelist = None


    def getSiteRequirements(self):
        """
        # // white list for anymatch clause
        """
        # turn resource control id's to list of ce's
        self.whitelist = getCEstrings(self.whitelist)
        anyMatchrequirements = ""
        if len(self.whitelist) > 0:
            anyMatchrequirements = " ("
            sitelist = ""
            #ces = getCEstrings(self.whitelist)
            for ce in self.whitelist:
                sitelist += "other.GlueCEUniqueID==\"%s\" || " % ce
            sitelist = sitelist[:-4]
            anyMatchrequirements += sitelist+")"
        return anyMatchrequirements



registerSubmitter(BlGLiteBulkResConSubmitter, BlGLiteBulkResConSubmitter.__name__)

