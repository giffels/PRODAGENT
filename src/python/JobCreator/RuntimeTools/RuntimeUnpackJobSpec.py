#!/usr/bin/env python
"""
_RuntimeUnpackJobSpec_

Unpack the details from the JobSpec file for this job and insert them
into this job

"""
import sys
import os
import pickle
from ProdCommon.FwkJobRep.TaskState import TaskState, getTaskState
from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from RunRes.RunResComponent import RunResComponent

from ProdCommon.TrivialFileCatalog import TrivialFileCatalog

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
        self.workflowSpec = WorkflowSpec()
        self.workflowSpec.load(os.environ["PRODAGENT_WORKFLOW_SPEC"])

        self.config = self.taskState.configurationDict()

        finder = NodeFinder(self.taskState.taskName())
        self.jobSpec.payload.operate(finder)
        self.jobSpecNode = finder.result

        wffinder = NodeFinder(self.taskState.taskName())
        self.workflowSpec.payload.operate(wffinder)
        self.workflowNode = wffinder.result

        tier0Merge = self.workflowSpec.parameters.get("Tier0Merge", "False")

        if self.jobSpecNode.jobType != "Merge" or tier0Merge == "True":

            if self.config.has_key('Configuration'):
                #try:
                self.createPSet()
                #except Exception, ex:
                #    msg = "Unable to generate cmsRun Config from JobSpec:\n"
                #    msg += str(ex)
                #    print msg
                #    badfile = open("exit.status", 'w')
                #    badfile.write("10040")
                #    badfile.close()

        else:
            #  //
            # // Merge job
            #//
            self.createMergePSet()

        # do after pset created to get correct input files
        self.setJobDetails()
        if self.config.has_key('UserSandbox'):
             self.userSandbox()

    def handleInputLink(self, config, inpLink):
        """
        _handleInputLink_

        Generate the information for the input link between this
        task and the task specified

        """
        msg = "Input Link Detected:\n"
        for k, v in inpLink.items():
            msg += " %s = %s\n" % (k, v)
        print msg

        inputTask = getTaskState(inpLink['InputNode'])

        if inputTask == None:
            msg = "Unable to create InputLink for task: %s\n" % (
                inpLink['InputNode'],)
            msg += "Input TaskState could not be retrieved..."
            raise RuntimeError, msg

        inputTask.loadJobReport()
        inputReport = inputTask.getJobReport()
        if inputReport == None:
            msg = "Unable to create InputLink for task: %s\n" % (
                inpLink['InputNode'],)
            msg += "Unable to load input job report file"
            raise RuntimeError, msg

        # add files to override catalog
        inputFileList = []
        tfc = None

        for file in inputReport.files:
            if not file['ModuleLabel'] == inpLink['OutputModule']:
                continue
            # link to file via lfn (in tfc) if link isn't standalone and we
            # have a valid lfn. Else refer to file via pfn
            if not inpLink['AppearStandalone'] and \
                            file.get('LFN', None) not in (None, '', 'None'):
                if not tfc:
                    tfc = TrivialFileCatalog.TrivialFileCatalog()
                inputFileList.append(file['LFN'])
                tfc.addLfnToPfnRule('override', file['LFN'], file['PFN'])
            else:
                inputFileList.append("file:%s" % file['PFN'])

        if tfc:
            print "Creating override tfc, contents below"
            print str(tfc)
            tfc.write(os.path.join(os.getcwd(), 'override_catalog.xml'))

        if inpLink['InputSource'] == "source":
            #  //
            # // feed into main source
            #//
            config.inputFiles = inputFileList
            if tfc:
                config.inputOverrideCatalog = os.path.join(os.getcwd(), 'override_catalog.xml')
            msg = "Input Link created to input source for files:\n"
            for f in inputFileList:
                msg += " %s\n" % f

            print msg
            return
        #  //
        # // Need to add to secondary source with name provided
        #//
        raise NotImplementedError, "Havent implemented secondary source input links at present..."


    def localCustomization(self, config, merge = False):
        """
        Apply site specific customizations to the config
        """
        site_config = self.taskState.getSiteConfig()

        self.ioCustomization(config, site_config.io_config, merge)


    def ioCustomization(self, config, custom_config, merge = False):
        """
        Apply site specific io customizations
        """
        # Don't do anything if no customization or job has no input files
        if not custom_config or (merge is False and not config.inputFiles):
            return

        import re
        version = lambda x: tuple(int(x) for x in re.compile('(\d+)').findall(x))
        cmssw_version = version(os.environ['CMSSW_VERSION'])

        # Only implemented in CMSSW_2_1_8 and above
        if cmssw_version < (2, 1, 8):
            return

        print "Site specific IO parameters will be used:"

        # cacheSize is a property of InputSource
        cache_size = custom_config.get('cacheSize', None)
        if cache_size:
            # Merge pset creates process on fly so can't use CMSSWConfig object
            if merge:
                from ProdCommon.CMSConfigTools.ConfigAPI.InputSource import InputSource
                inputSource = InputSource(config.source)
                inputSource.setCacheSize(cache_size)
            else:
                config.sourceParams['cacheSize'] = cache_size

        if merge:
            from FWCore.ParameterSet.Modules import Service
            config.add_(Service('AdaptorConfig'))

        for param in custom_config:
            print "  %s %s" % (param, custom_config[param])
            if param == 'cacheSize':
                continue

            if merge:
                import FWCore.ParameterSet.Types as CfgTypes
                adaptor = config.services['AdaptorConfig']
                setattr(adaptor, param,
                        CfgTypes.untracked(CfgTypes.string(str(custom_config[param]))))
            else:
                config.tFileAdaptorConfig[param] = custom_config[param]
        return


    def createPSet(self):
        """
        _createPSet_

        Create the PSet cfg File

        """
        cfgFile = self.config['Configuration'].get("CfgFile", "PSet.py")[0]
        cfgFile = str(cfgFile)
        self.jobSpecNode.loadConfiguration()
        self.jobSpecNode.cfgInterface.rawCfg = self.workflowNode.cfgInterface.rawCfg

        # taken from cmssw environment
        # pylint: disable-msg=F0401
        import FWCore.ParameterSet.Types as CfgTypes
        # pylint: enable-msg=F0401

        workingDir = os.path.join(os.getcwd(), 'prestage')
        if os.path.exists(workingDir + '/prestageTFC.xml'):

           rawCfg = pickle.loads(self.jobSpecNode.cfgInterface.rawCfg)
           rawCfg.source.overrideCatalog = CfgTypes.untracked(CfgTypes.string('trivialcatalog_file:%s/prestageTFC.xml?protocol=local-stage-in' % workingDir))

           self.jobSpecNode.cfgInterface.rawCfg = pickle.dumps(rawCfg)

        # Apply site specific customizations
        self.localCustomization(self.jobSpecNode.cfgInterface)

        for inpLink in self.jobSpecNode._InputLinks:
            #  //
            # // We have in-job input links to be resolved
            #//
            self.handleInputLink(self.jobSpecNode.cfgInterface, inpLink)

        cmsProcess = self.jobSpecNode.cfgInterface.makeConfiguration()



        pycfgDump = open("PyCfgFileDump.log", 'w')
        try:
            pycfgDump.write(cmsProcess.dumpPython())
        except Exception, ex:
            msg = "Error writing python format cfg dump:\n"
            msg += "%s\n" % str(ex)
            msg += "This needs to be reported to the framework team"
            pycfgDump.write(msg)
        pycfgDump.close()

        handle = open(cfgFile, 'w')
        handle.write("import pickle\n")
        handle.write("pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps(cmsProcess))
        handle.write("process = pickle.loads(pickledCfg)\n")
        handle.close()

        return


    def createMergePSet(self):
        """
        _createMergePSet_

        Merges are a little different since we have to build the entire
        process object from scratch.

        """
        print "<<<<<<<<<<<<<<<<<<<<Merge>>>>>>>>>>>>>>>>>>>>>."
        cfgFile = self.config['Configuration'].get("CfgFile", "PSet.py")[0]
        cfgFile = str(cfgFile)
        self.jobSpecNode.loadConfiguration()
        cfgInt = self.jobSpecNode.cfgInterface

        # taken from cmssw environment
        # pylint: disable-msg=F0401
        from FWCore.ParameterSet.Config import Process, EndPath
        from FWCore.ParameterSet.Modules import OutputModule, Source
        import FWCore.ParameterSet.Types as CfgTypes
        import FWCore.ParameterSet.Config as cms
        # pylint: enable-msg=F0401

        processName = "Merge"
        process = Process(processName)
        process.source = Source("PoolSource")
        process.source.fileNames = CfgTypes.untracked(CfgTypes.vstring())
        for entry in cfgInt.inputFiles:
            process.source.fileNames.append(str(entry))

        # For StoreResults, drop DQM products as they are sometimes left over
        if cfgInt.configMetadata.get("annotation",None) \
            == "AutoGenerated By StoreResults":
            inputCommands = getattr(process.source,'inputCommands',
                                    cms.untracked.vstring('keep *'))
            inputCommands.append('drop *_EDMtoMEConverter_*_*')
            process.source.inputCommands = inputCommands

        outMod = cfgInt.outputModules['Merged']
        process.Merged = OutputModule("PoolOutputModule")
        process.Merged.fileName = CfgTypes.untracked(CfgTypes.string(
            outMod['fileName']))

        process.Merged.logicalFileName = CfgTypes.untracked(CfgTypes.string(
            outMod['logicalFileName']))

        process.Merged.catalog = CfgTypes.untracked(CfgTypes.string(
            outMod['catalog']))

        process.outputPath = EndPath(process.Merged)

        # Apply site specific customizations
        self.localCustomization(process, merge=True)

    

        pycfgDump = open("PyCfgFileDump.log", 'w')
        try:
            pycfgDump.write(process.dumpPython())    
        except Exception, ex:
            msg = "Error writing python format cfg dump:\n"
            msg += "%s\n" % str(ex)
            msg += "This needs to be reported to the framework team"
            pycfgDump.write(msg)
        pycfgDump.close()

        handle = open(cfgFile, 'w')
        handle.write("import pickle\n")
        handle.write("pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps(process))
        handle.write("process = pickle.loads(pickledCfg)\n")
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
        if inpSrc.has_key('firstRun'):
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
    workflowSpec = os.environ.get("PRODAGENT_WORKFLOW_SPEC", None)
    if workflowSpec == None:
        msg = "Unable to find WorkflowSpec from PRODAGENT_WORKFLOW_SPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    if not os.path.exists(jobSpec):
        msg = "Cannot find JobSpec file:\n %s\n" % jobSpec
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    instance = JobSpecExpander(jobSpec)
