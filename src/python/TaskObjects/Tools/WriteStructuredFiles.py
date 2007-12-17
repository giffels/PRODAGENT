#!/usr/bin/env python
"""
_WriteStructuredFiles_


"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: WriteStructuredFiles.py,v 1.1 2006/04/10 17:40:38 evansde Exp $"
__author__ = "evansde@fnal.gov"

import os
from TaskObjects.StructuredFile import StructuredFile


class WriteStructuredFiles:
    """
    _WriteStructuredFiles_

    """
  

    def __call__(self, taskObject):
        """
        

        """
        if taskObject.get('Directory', None) == None:
            return
        
        
        workingDir =  taskObject['Directory'].physicalPath
        

        if not os.path.exists(workingDir):
            msg =  "ERROR: Cannot create IMProvDocs in dir:\n"
            msg += "%s\n" % workingDir
            msg += "since it does not exist..."
            raise RuntimeError, msg
        
        for structFile in taskObject["StructuredFiles"]:
            value = taskObject.get(structFile, None)
            if value == None:
                continue
            if value.__class__.__name__ != StructuredFile.__name__:
                msg = "ERROR: Non-StructuredFile object:\n"
                msg += "Key: %s is registered as a StructuredFile\n" % (
                    structFile,
                    )
                msg += "but is not a StructuredFile instance"
                raise RuntimeError, msg

            value.setTargetDir(workingDir)
            value.write()
        return
            
