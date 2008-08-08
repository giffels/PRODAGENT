#!/usr/bin/env python
"""
_JobEmulatorBulkSubmitter_

Job Emulator Bulk Submitter implementation.

"""

__revision__ = "$Id: JobEmulatorBulkSubmitter.py,v 1.2 2008/02/29 22:01:42 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import logging

from MessageService.MessageService import MessageService

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface

class JobEmulatorBulkSubmitter(BulkSubmitterInterface):
    """
    _JobEmulatorBulkSubmitter_

    """
    def doSubmit(self):
        """
        _doSubmit_

        Perform bulk or single submission as needed based on the class data
        populated by the component that is invoking this plugin
        """

        # create message service
        ms = MessageService()
                                                                                
        # register
        ms.registerAs("JobEmulatorBulkSubmitter")

        for jobSpec, cacheDir in self.toSubmit.items():
            logging.debug("SpecFile = %s" % self.specFiles[jobSpec])
            ms.publish("EmulateJob", self.specFiles[jobSpec])
            ms.commit()
            logging.debug("EmulateJob message sent")
        return

    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Make sure config has what is required for this submitter

        """
        if self.pluginConfig == None:
            msg = "Failed to load Plugin Config for:\n"
            msg += self.__class__.__name__
            return

registerSubmitter(JobEmulatorBulkSubmitter, JobEmulatorBulkSubmitter.__name__)
