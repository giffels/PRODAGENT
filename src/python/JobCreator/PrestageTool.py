#!/usr/bin/env python
"""
_PrestageTool_

Tool to add prestage tools to the job 
"""

import os
import inspect
import logging
import JobCreator.RuntimeTools.RuntimePrestage as Prestage
 

class InstallPrestage:
      """
      Install the script that stages in job's input data to local WN before job actually runs by cmsrun. 
      Creates on the fly TFC which contains lfn to local PFN mapping and add overridecatalog parameter to process
      source block of cfg   
      """ 
      def __call__ (self, taskObject):
          """
          Making that class callable
          """


          # // Install the script as a PreApp command
        
          if taskObject['Type'] != "CMSSW":
             return

          if taskObject.get('PreStage','false').lower() == 'false':
             return

                      
          srcfile = inspect.getsourcefile(Prestage)
          if not os.access(srcfile, os.X_OK):
              os.system("chmod +x %s" % srcfile)
          taskObject.attachFile(srcfile)

          taskObject['PreAppCommands'].append(
             "./RuntimePrestage.py"
             )
          

          return
