#/usr/bin/env python
"""
_FastMergeTools_

Utils for configuring fast merge jobs

"""
import logging
from ProdCommon.CMSConfigTools.CfgInterface import CfgInterface

def installFastMerge(taskObject):
    """
    _installFastMerge_

    Install all the necessary pieces for the EdmFastMerge to run

    """
    logging.debug("Installing fast merge for task: %s" % taskObject['Name'])
    
    pythonPSet = taskObject['CMSPythonPSet']
    taskObject['CMSPythonPSet'] = None
    
    

    cfgInt = CfgInterface(pythonPSet, True)
    inputFiles = cfgInt.inputSource.fileNames()

    outMod = cfgInt.outputModules['Merged']
    lfn = outMod.logicalFileName()
    catalog = outMod.catalog()
    pfn = outMod.fileName()

    pfn = pfn.replace("\'", "")
    pfn = pfn.replace("\"", "")
    lfn = lfn.replace("\'", "")
    lfn = lfn.replace("\"", "")
    catalog = catalog.replace("\'", "")
    catalog = catalog.replace("\"", "")

    
    commandLineArgs = " -o %s -l %s " % (pfn, lfn) 
    commandLineArgs += " -j FrameworkJobReport.xml "
    commandLineArgs += " -w %s -i " % catalog

    for inputfile in inputFiles:
        inputfile = inputfile.replace("\'", "")
        inputfile = inputfile.replace("\"", "")
        commandLineArgs += "%s " % inputfile

    taskObject['CMSCommandLineArgs'] = commandLineArgs

    return
    
    
