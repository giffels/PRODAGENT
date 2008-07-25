#!/usr/bin/env python

import os
import logging

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.GLiteBulkInterface import GLiteBulkInterface

from  ProdAgent.ResourceControl.ResourceControlAPI import createCEMap

# inherit from base class and override any needed methods

class GLiteBulkResConSubmitter(GLiteBulkInterface):
    """
    Class to do GLite bulk submission with sites
    listed in resource control db
    """
    
    def getSiteRequirements(self):
        """
        form site requirement list
        """
        anyMatchrequirements = ""
        if len(self.whitelist)>0:
            Whitelist = self.whitelist
            anyMatchrequirements = " ("
            sitelist = ""
            cemap = createCEMap(activeOnly=False)
            for i in Whitelist:
                try:
                    name = cemap[int(i)]
                    sitelist += "other.GlueCEUniqueID==\"%s\" || " % name
                    logging.debug("Whitelist element %s" % name)
                except Exception, ex:
                    raise RuntimeError("Error mapping site id %s to ce: %s" % (str(i), str(ex)))
            sitelist = sitelist[:len(sitelist)-4]
            anyMatchrequirements+=sitelist+")"
        return anyMatchrequirements


registerSubmitter(GLiteBulkResConSubmitter, GLiteBulkResConSubmitter.__name__)