#!/usr/bin/env python
"""
_BulkTools_

Utils for Bulk style Jobs

"""

import os
import inspect
import logging
import JobCreator.RuntimeTools.RuntimeUnpackJobSpec as Unpacker



class InstallUnpacker:
    """
    _InstallUnpacker_

    Install the script that unpacks the JobSpec into the
    cfg and RunResDB for bulk job CMSSW nodes

    """
    def __call__(self, taskObject):
        #  //
        # // Install the script as a PreTask command
        #//
        if taskObject['Type'] != "CMSSW":
            return
        srcfile = inspect.getsourcefile(Unpacker)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)
        
        taskObject['PreTaskCommands'].append(
            "./RuntimeUnpackJobSpec.py"
            )
        
        
class InstallUserSandbox:

    def __call__(self, taskObject):
        if taskObject['Type'] != "CMSSW":
            return
        node = taskObject['PayloadNode']
        if node.userSandbox == None:
            return

        if not os.path.exists(node.userSandbox):
            msg = "Warning: Cannot find specified User Sandbox:\n"
            msg += "%s\n" % node.userSandbox
            logging.warning(msg)
            return
        
        
        
        taskObject.attachFile(node.userSandbox)

        runres = taskObject['RunResDB']
        
        runres.addData("/%s/UserSandbox" % taskObject['Name'],
                       os.path.basename(node.userSandbox)
                       )
        return
                       
