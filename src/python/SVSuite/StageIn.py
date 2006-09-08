#!/usr/bin/env python
"""
_StageIn_

Utilities to perform a stage in for a file based on LFN, do a
TFC lookup and then grab the file using the appropriate command.

At present, a really nasty regexp match on the url is used to
determine the command to be used, but it will do for prototyping...

"""

import re
import os

from SVSuite.SVSuiteError import SVSuiteStageInFailure, SVSuiteToolFailure
from SVSuite.Execute import execute

from FwkJobRep.SiteLocalConfig import loadSiteLocalConfig
from FwkJobRep.TrivialFileCatalog import loadTFC

def stageInFile(lfn, targetDir):
    """
    _stageInFile_

    Match the LFN to PFN and try to do the stage in based on the
    data in the local siteconf and TFC to get a PFN.

    This is the main public API for all the stage in operations.

    This method will return the local PFN of the file after stage in upon
    success.

    It will throw a StageInError if the stage in cannot be completed.
    

    """
    targetFile = os.path.join(targetDir, os.path.basename(lfn))
    try:
        siteconf = loadSiteLocalConfig()
    except Exception, ex:
        msg = "Unable to load SiteConfig file:\n"
        msg += str(ex)
        raise SVSuiteStageInFailure(msg, LFN = lfn)

    tfcUrl = siteconf.eventData.get('catalog', None)
    if tfcUrl == None:
        msg = "No Catalog URL found for event-data in Site Config:\n"
        msg += "Cannot proceed with LFN->PFN translation\n"
        raise SVSuiteStageInFailure(msg,
                                    LFN = lfn ,
                                    EventData = siteconf.eventData)
    
    try:
        tfc = loadTFC(tfcUrl)
    except Exception, ex:
        msg = "Failed to load Trivial File Catalog:\n"
        msg += "%s\n" % tfcUrl
        msg += str(ex)
        raise SVSuiteStageInFailure(msg,
                                    LFN = lfn , TFCUrl = tfcUrl)
    
    
    pfn = tfc.matchLFN(tfc.preferredProtocol, lfn)
    if pfn == None:
        msg = "Unable to map LFN to PFN:\n"
        msg += "LFN: %s\n" % lfn
        raise SVSuiteStageInFailure(msg,
                                    LFN = lfn , TFCUrl = tfcUrl,
                                    TFCContent = str(tfc) )
    
    

    command = makeCommand(pfn, targetFile)

    try:
        execute(command)
    except SVSuiteToolFailure, ex:
        msg = "Failed to run Stage In Command:\n"
        msg += "%s \n" % command
        msg += str(ex)
        raise SVSuiteStageInFailure(msg, LFN = lfn, TargetFile = targetFile)
    
    return targetFile
    

#  //==================HACK!==============================
# // 
#//  Stuff for doing stage in command generation below is just a temp
#  //Hack to get things started.
# // A StageIn package much like the StageOut package in ProdAgent should
#//  probably be developed for this...

def makeSRMCommand(pfn, targetFile):
    """
    _makeSRMCommand_

    Make an srmcp stage in command

    """
    result = "srmcp -debug %s file:///%s" % (pfn, targetFile)
    return result

def makeDCCPCommand(pfn, targetFile):
    """
    _makeDCCPCommand_

    Make a dccp stage in command

    """
    result = "dccp %s %s" % (pfn, targetFile)
    return result

    
    
#  //
# //  Regexp to command mapping 
#//
#  // If the regexp matches the PFN, the command associated with the
# //  regexp is used to attempt the stage in.
#//   
_ProtocolMatch = {
    "srm" : re.compile("srm://"),
    "dccp" : re.compile("dcap://"),
    
    }

_CommandMaker = {
    "srm" : makeSRMCommand,
    "dccp" : makeDCCPCommand,
    }


def makeCommand(pfn, targetFile):
    """
    _makeCommand_

    Guess the stage in protocol from the pfn, and based on the Protocol
    Matched, generate the appropriate command
    
    """
    protocol = None
    for proto, regexp in _ProtocolMatch.items():
        if regexp.match(pfn):
            protocol = proto
            break

    if protocol == None:
        msg = "Unable to match protocol for file PFN:\n"
        msg += "%s\n" % pfn
        raise SVSuiteStageInFailure(msg, PFN = pfn)

    maker = _CommandMaker.get(protocol, None)
    if maker == None:
        msg = "Unable to find command maker for protocol: %s\n" % protcol
        msg += "%s\n" % pfn
        raise SVSuiteStageInFailure(msg, PFN = pfn, Protocol = protocol,
                                    KnownProtocols = _CommandMaker.keys(),
                                    )
    try:
        command = maker(pfn, targetFile)
    except Exception, ex:
        msg = "Error Creating Stage In command for PFN:\n"
        msg += "%s\n" % pfn
        msg += "With protocol: %s\n" % protocol
        msg += str(ex)
        raise SVSuiteStageInFailure(msg, PFN = pfn, Protocol = protocol)
    return command
