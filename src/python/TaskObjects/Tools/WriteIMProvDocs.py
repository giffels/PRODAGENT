#!/usr/bin/env python
"""
_WriteIMProvDocs_

Operator for writing out IMProvDocs contained in a TaskObject.

All keys listed in the TaskObjects WriteIMProv list are retrieved and
written into XML files in the (pre-existing) directory associated with
that task object.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: WriteIMProvDocs.py,v 1.1 2006/04/10 17:40:38 evansde Exp $"
__author__ = "evansde@fnal.gov"

import os
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvNode import IMProvNode



class WriteIMProvDocs:
    """
    _WriteIMProvDocs_

    Operator to act on a TaskObject and write all the IMProvDoc instances
    it contains to files in the directory specified by the
    TaskObjects Directory attribute.

    """

    def __call__(self, taskObject):
        """
        _operator()_

        Act on a taskObject to extract the directory where files are to be
        written and process the IMProvDocs list to write all registered
        IMProvDoc instances into files in the TaskObject directory

        """
        if taskObject.get('Directory', None) == None:
            return
        
        
        workingDir = taskObject['Directory'].physicalPath
       

        if not os.path.exists(workingDir):
            msg =  "ERROR: Cannot create IMProvDocs in dir:\n"
            msg += "%s\n" % workingDir
            msg += "since it does not exist..."
            raise RuntimeError, msg

        for item in taskObject['IMProvDocs']:
            value = taskObject.get(item, None)
            if value == None:
                continue

            targetFile = os.path.join(workingDir, "%s.xml" % item)

            
            if value.__class__.__name__ == IMProvDoc.__name__:
                handle = open(targetFile, 'w')
                dom = value.makeDOMDocument()
                handle.write(dom.toprettyxml())
                handle.close()
            elif value.__class__.__name__ == IMProvNode.__name__:
                handle = open(targetFile, 'w')
                dom = value.makeDOMElement()
                handle.write(dom.toprettyxml())
                handle.close()
            elif isinstance(value, IMProvNode):
                handle = open(targetFile, 'w')
                dom = value.makeDOMElement()
                handle.write(dom.toprettyxml())
                handle.close()
            else:
                msg = "Object is not an IMProv Object:\n"
                msg += "Object referenced by key: %s\n" % item
                raise RuntimeError, msg
        return
    
