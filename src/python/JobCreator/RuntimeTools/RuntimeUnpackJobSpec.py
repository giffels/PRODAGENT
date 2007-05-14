#!/usr/bin/env python
"""
_RuntimeUnpackJobSpec_

Unpack the details from the JobSpec file for this job and insert them
into this job

"""
import sys
import os
from FwkJobRep.TaskState import TaskState
from ProdCommon.MCPayloads.JobSpec import JobSpec
from RunRes.RunResComponent import RunResComponent

class NodeFinder:

    def __init__(self, nodeName):
        self.nodeName = nodeName
        self.result = None

    def __call__(self, nodeInstance):
        if nodeInstance.name == self.nodeName:
            self.result = nodeInstance

def unquote(strg):
    """remove leading and trailing quotes from string"""
    while strg.startswith("\'") or strg.startswith("\""):
        strg = strg[1:]
    while strg.endswith("\'") or strg.endswith("\""):
        strg = strg[:-1]
    return strg   


class JobSpecExpander:

    def __init__(self, jobSpecFile):
        self.jobSpec = JobSpec()
        self.jobSpec.load(jobSpecFile)
        self.taskState = TaskState(os.getcwd())
        self.taskState.loadRunResDB()
        
        
        self.config = self.taskState.configurationDict()

        finder = NodeFinder(self.taskState.taskName())
        self.jobSpec.payload.operate(finder)
        self.jobSpecNode = finder.result

        self.setJobDetails()
            
        if self.config.has_key('Configuration'):
            try:
                self.createPSet()
            except Exception, ex:
                msg = "Unable to generate cmsRun Config from JobSpec:\n"
                msg += str(ex)
                print msg
                badfile = open("exit.status", 'w')
                badfile.write("10040")
                badfile.close()

        if self.config.has_key('UserSandbox'):
            self.userSandbox()
        
            


    def createPSet(self):
        """
        _createPSet_

        Create the PSet cfg File

        """
        cfgFile = self.config['Configuration'].get("CfgFile", "PSet.cfg")[0]
        cfgFile = str(cfgFile)
        self.jobSpecNode.loadConfiguration()
        cmsProcess = self.jobSpecNode.cfgInterface.makeConfiguration()

        handle = open(cfgFile, 'w')
        handle.write(cmsProcess.dumpConfig())
        handle.close()
        
        return
        

    def userSandbox(self):
        """
        _userSandbox_

        If a User Sandbox is specified, unpack it

        """
        try:
            sandboxName = self.config['UserSandbox'][-1]
            command = "tar -zxf %s" % sandboxName
            os.system(command)
        except Exception, ex:
            msg = "Error handling user sandbox:\n"
            msg += str(ex)
            print msg
        return
        

    def setJobDetails(self):
        """
        _setJobName_

        Propagate Job Information from JobSpec to RunResDB
        
        """
        
        self.config['JobSpecID'][0] = self.jobSpecNode.jobName
        self.jobSpecNode.loadConfiguration()
        cfgInt = self.jobSpecNode.cfgInterface
        inpSrc = cfgInt.sourceParams 
        if  self.config['Input'].has_key("MaxEvents"):
            del self.config['Input']['MaxEvents']
        self.config['Input']['MaxEvents'] = [cfgInt.maxEvents['input']]
        if self.config['Input'].has_key("FirstRun"):
            del self.config['Input']['FirstRun']
        self.config['Input']['FirstRun'] = [inpSrc['firstRun']]
        if self.config['Input'].has_key("SourceType"):
            del self.config['Input']['SourceType']
        self.config['Input']['SourceType'] = [cfgInt.sourceType]

        self.config['Input']['InputFiles'] = []
        
        inpFileList = cfgInt.inputFiles

        for inpFile in inpFileList:
            self.config['Input']['InputFiles'].append(
                inpFile.replace("\'", "")
                )
                
            
        for modName, item in cfgInt.outputModules.items():
            if item.get('catalog', None) == None:
                continue
            catalog = unquote(item['catalog'])
            catalog = os.path.join(self.taskState.dir, catalog)
            if not self.config['Output']['Catalogs'].has_key(modName):
                self.config['Output']['Catalogs'][modName] = []
            self.config['Output']['Catalogs'][modName].append(catalog)

            
        #  //
        # // Now save the RunResDB with the updates
        #//
        newComponent = RunResComponent()
        dictRep = {"%s" % self.taskState.taskName() : self.config }
        newComponent.populate(dictRep)

        targetFile = os.path.join(self.taskState.dir, "RunResDB.xml")
        handle = open(targetFile, 'w')
        dom = newComponent.makeDOMElement()
        handle.write(dom.toprettyxml())
        handle.close()
        return
        
        
if __name__ == '__main__':
   
    jobSpec = os.environ.get("PRODAGENT_JOBSPEC", None)
    if jobSpec == None:
        msg = "Unable to find JobSpec from PRODAGENT_JOBSPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    if not os.path.exists(jobSpec):
        msg += "Cannot find JobSpec file:\n %s\n" % jobSpec
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    instance = JobSpecExpander(jobSpec)
    
    
    
        
