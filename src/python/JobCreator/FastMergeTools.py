#/usr/bin/env python
"""
_FastMergeTools_

Utils for configuring fast merge jobs

"""
import os
import inspect
import logging


import JobCreator.RuntimeTools.RuntimeUnpackFastMerge as Unpacker

def installFastMerge(taskObject):
    """
    _installFastMerge_

    Install all the necessary pieces for the EdmFastMerge to run

    """
    logging.debug("Installing fast merge for task: %s" % taskObject['Name'])
    
    pythonPSet = taskObject['CMSPythonPSet']
    taskObject['CMSPythonPSet'] = None
    
    if taskObject.has_key("JobSpecNode"):
        cfgInt = taskObject['JobSpecNode'].cfgInterface
    if taskObject.has_key("PayloadNode"):
        cfgInt = taskObject['PayloadNode'].cfgInterface

        

    inputFiles = cfgInt.inputFiles

    outMod = cfgInt.outputModules['Merged']
    lfn = outMod['ogicalFileName']
    catalog = outMod['catalog']
    pfn = outMod['fileName']

    pfn = pfn.replace("\'", "")
    pfn = pfn.replace("\"", "")
    lfn = lfn.replace("\'", "")
    lfn = lfn.replace("\"", "")
    catalog = catalog.replace("\'", "")
    catalog = catalog.replace("\"", "")
    
    
    commandLineArgs = " -k -o %s -l %s " % (pfn, lfn) 
    commandLineArgs += " -j FrameworkJobReport.xml "
    commandLineArgs += " -w %s -i " % catalog
    
    for inputfile in inputFiles:
        inputfile = inputfile.replace("\'", "")
        inputfile = inputfile.replace("\"", "")
        commandLineArgs += "%s " % inputfile

    taskObject['CMSCommandLineArgs'] = commandLineArgs

    return
    
    
class InstallBulkFastMerge:
    """
    _InstallBulkFastMerge_

    Install all the necessary pieces for the EdmFastMerge to run
    in a Bulk created job
    """

    def __call__(self, taskObject):
        
        if taskObject['Type'] != "CMSSW":
            return
        if taskObject['CMSExecutable'] != "EdmFastMerge":
            return
        
        logging.debug("Installing fast merge for task: %s" % (
            taskObject['Name'],)
                      )
        
        pythonPSet = taskObject['CMSPythonPSet']
        taskObject['CMSPythonPSet'] = None
        
        
        taskObject['PreAppCommands'].append(" . ./EdmFastMerge-setup.sh")
        
    
        commandLineArgs = " -o $EDM_MERGE_OUTPUT_PFN "
        commandLineArgs += "-l $EDM_MERGE_OUTPUT_LFN " 
        commandLineArgs += " -j FrameworkJobReport.xml "
        commandLineArgs += " -w $EDM_MERGE_CATALOG "
        commandLineArgs += " -i $EDM_MERGE_INPUTFILES" 
        
        taskObject['CMSCommandLineArgs'] = commandLineArgs


        srcfile = inspect.getsourcefile(Unpacker)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)
        
        taskObject['PreTaskCommands'].append(
            "./RuntimeUnpackFastMerge.py"
            )
        
        return
        
    
