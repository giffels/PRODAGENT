#!/usr/bin/env python
"""
_MakeRelValSpec_

Utils for generating RelVal Specs from Release Validation config files
& pickle files
"""


import os
import sys
import pickle


from RelValInjector.RelValSpecMgr import RelValTest
from IMProv.IMProvDoc import IMProvDoc

def pickleConfigFile(cfgFilePath, pickleFile = None):
    """
    _pickleConfigFile_

    Given the cfg file provided, take that file and
    generate a pickled version of the file.

    Path to the pickle file is returned

    """
    from FWCore.ParameterSet.Config import include as PSetInclude
    process = PSetInclude(cfgFilePath)
    if pickleFile == None:
        pickleFile = "%s.PKL" % cfgFilePath
    handle = open(pickleFile, 'w')
    pickle.dump(process, handle)
    handle.close()
    return pickleFile



def makeRelValTest(pickleFile, testName, **args):
    """
    _makeRelValTest_

    Create a RelValTest instance

    """
    nevents = args.get("TotalEvents", None)
    if nevents == None:
        msg = "TotalEvents argument not provided\n"
        msg += "This is required"
        raise RuntimeError, msg
    

    speed = args.get("SpeedCategory", "Slow")
    selEff = args.get("SelectionEfficiency", None)
    inpDataset = args.get("InputDataset", None)

    newTest = RelValTest()
    newTest["Name"] = testName
    newTest["SpeedCategory"] = speed
    newTest["TotalEvents"] = nevents

    
    
    newTest["SelectionEfficiency"] = selEff
    newTest["PickleFile"] = pickleFile
    newTest["InputDataset"] = inpDataset

    cmsswVersion = args.get("CMSSWVersion", None)
    if cmsswVersion == None:
        var = os.environ.get('CMSSW_BASE', None)
        if var != None:
            cmsswVersion = os.path.basename(var)
    cmsPath = args.get("CMSPath", None)
    if cmsPath == None:
        var = os.environ.get("CMS_PATH", None)
        if var != None:
            cmsPath = var

    scramArch = args.get("CMSSWArchitecture", None)
    if scramArch == None:
        var = os.environ.get("SCRAM_ARCH", None)
        if var != None:
            scramArch = var
    
    newTest["CMSSWVersion"] = cmsswVersion
    newTest["CMSSWArchitecture"] = scramArch
    newTest["CMSPath"] = cmsPath

    return newTest


def writeSpecFile(filename, *tests):
    """
    _writeSpecFile_

    Gather a list of tests into a RelVal Spec file suitable for
    injection into a PA

    """
    doc = IMProvDoc("RelValSpec")
    for test in tests:
        doc.addNode(test.save())
    handle = open(filename, 'w')
    handle.write(doc.makeDOMDocument().toprettyxml())
    handle.close()
    return

    


        

        
