#!/usr/bin/env python
"""
_cfgFromWorkflow_

Tool to extract cfg content from a workflow

"""

import sys
import os
import getopt

valid = [ "workflow=", "make-index" ]

usage = "Usage: cfgFromWorkflow.py --workflow=<workflow file>\n"
usage += "                         --make-index\n"
usage += "      Will create a workflow.nodename.cfg file for each cfg\n"
usage += "      Found within the workflow provided\n"
usage += "      If --make-index is provided, an index file for each cfg\n"
usage += "      Will also be generated\n"



workflow = None
doIndex = False


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)


for opt, arg in opts:
    if opt == "--workflow":
        workflow = arg
    if opt == "--make-index":
        doIndex = True



from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig




if workflow == None:
    msg = "Error: --workflow option is required"
    raise RuntimeError, msg

if not os.path.exists(workflow):
    msg = "Cannot find workflow file:\n %s" % workflow
    raise RuntimeError, msg

spec = WorkflowSpec()
spec.load(workflow)





def indexDict(modName, modRef):
    """
    _indexDict_

    Given a PSet like python dictionary, generate an index file from
    it. If it has PSet children, act on them recursively

    """
    result = []
    #  //
    # // Traverse module parameters
    #//
    for key, value in modRef.items():
        #  //
        # // Is this a PSet? If so descend into it
        #//
        isPset = value[0] == "PSet"
        if not isPset:
            #  //
            # // This is not a PSet, index it
            #//  Drop it if it is untracked, or an internal key (@)
            if key.startswith("@"):
                continue
            if value[1] == "untracked":
                continue
            result.append((value[0], "%s.%s=%s" % ( modName, key, value[2])))
        else:
            #  //
            # // Recursively descend into PSet
            #//
            children = indexDict(key, value[2])
            [ result.append( (i[0], "%s.%s" % (modName, i[1]))) for i in children ]
            
    return result
    



def actOnCfg(nodename, cfgInt):
    """
    _actOnCfg_

    Found a cfg, write it out as cfg string and make an index file
    if required.

    """
    cfgFile = "%s.%s.py" % (os.path.basename(workflow), nodename)
    handle = open(cfgFile, 'w')
    handle.write(cfgInt.dumpPython())
    handle.close()
    print "Wrote cfg file: %s" % cfgFile

    if not doIndex:
        return
    print "Generating Index..."
    indices = [("PSetHash", "PSetHash=\"%s\"" % hashValue)]
    #  //
    # // Go through module by module and generate index
    #//
    for moduleName in cfgInt.cmsConfig.moduleNames():
        print "  Indexing Module: %s" % moduleName
        moduleRef = cfgInt.cmsConfig.module(moduleName)
        indexParams = indexDict(moduleName, moduleRef)
        indices.extend(indexParams)
    print "Found %s Indices" % len(indices)

    #  //
    # // Write index file
    #//
    indexFile = "%s.index" % cfgFile
    handle = open(indexFile, 'w')
    for line in indices:
        handle.write("%s %s\n" % (line[0], line[1]))
    handle.close()
    print "Wrote index file: %s" % indexFile
    return
    
def findCfgFiles(node):
    """
    _findCfgFiles_

    Look for cms cfg file in payload node provided

    """
    try:
        #hash = node._OutputDatasets[0]['PSetHash'] 
        #cfg = node.configuration
        cfg = node.cfgInterface
        cfgInter = cfg.makeConfiguration()
        print node.name + ": Found cfg."
    except Exception, ex:
        # Not a cfg file
        print ex
        print node.name + ": No cfg. found."
        return
    
    actOnCfg(node.name, cfgInter)
    return

    
spec.payload.operate(findCfgFiles)

