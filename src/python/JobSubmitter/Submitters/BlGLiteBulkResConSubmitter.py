#!/usr/bin/env python
"""
python

Glite Collection class

"""

__revision__ = "$Id: BlGLiteBulkResConSubmitter.py,v 1.4 2009/03/17 14:11:59 gcodispo Exp $"
__version__ = "$Revision: 1.4 $"

import logging

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BlGLiteBulkSubmitter import BlGLiteBulkSubmitter

from JobQueue.JobQueueDB import JobQueueDB


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
        self.jobQ = JobQueueDB()


    def getSiteRequirements(self):
        """
        # // white list for anymatch clause
        """
        anyMatchrequirements = ""
        if len(self.whitelist) > 0:
            used_names = []
            anyMatchrequirements = " ("
            sitelist = ""
            for id in self.whitelist:
                sites = self.jobQ.getSite(id)  
                for site in sites:
                    if site["SiteName"] in used_names:
                        continue
                    if site['CEName']:
                        sitelist += "other.GlueCEUniqueID==\"%s\" || " % site['CEName']
                    else:
                        sitelist += " Member(\"%s\", other.GlueCESEBindGroupSEUniqueID) || " % site["SEName"]
                    used_names.append(site["SiteName"])       
            
            if not used_names:
                raise RuntimeError, "Unable to map whitelist to site: %s" % str(self.whitelist)  
                
            sitelist = sitelist[:-4]
            anyMatchrequirements += sitelist+")"
            self.whitelist = used_names # publish to dashboard later
        
        return anyMatchrequirements



registerSubmitter(BlGLiteBulkResConSubmitter, BlGLiteBulkResConSubmitter.__name__)

