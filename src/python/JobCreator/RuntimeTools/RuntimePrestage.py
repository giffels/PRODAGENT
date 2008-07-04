#!/usr/bin/env python
"""
_RuntimeT0Prestage_

Tier 0 Prestage script.

Runs through list of input LFNs.
Prestages the LFNs to the local WN.
Updates the cfg to use the local PFN to read the file


"""

import os

from ProdCommon.MCPayloads.JobSpec import JobSpec
import popen2
import fcntl, select, sys
import time

from ProdCommon.FwkJobRep.TaskState import TaskState
from ProdCommon.FwkJobRep.TrivialFileCatalog import tfcFilename, tfcProtocol, readTFC, loadTFC
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvNode import IMProvNode

from IMProv.IMProvLoader import loadIMProvFile

def makeNonBlocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
	fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)



class Prestager:
    """
    _Prestager_

    Castor Prestager

    """
    
    def __init__(self):
        self.state = TaskState(os.getcwd())

        self.siteConf = self.state.getSiteConfig()
           

        self.spec = JobSpec()
        self.numberOfRetries = 3 
        self.retryInterval = 600 #seconds
        jobSpec = os.environ.get("PRODAGENT_JOBSPEC", None)
        if jobSpec == None:
            msg = "Unable to find JobSpec from PRODAGENT_JOBSPEC variable\n"
            msg += "Unable to proceed\n"
            raise RuntimeError, msg

        if not os.path.exists(jobSpec):
            msg += "Cannot find JobSpec file:\n %s\n" % jobSpec
            msg += "Unable to proceed\n"
            raise RuntimeError, msg
        self.specFile = jobSpec
        self.spec.load(jobSpec)

        self.spec.payload.loadConfiguration()
        self.files = self.spec.payload.cfgInterface.inputFiles


        print self.files
        self.lfnToPfn = {}
        self.localFiles = {}

    def executeCommand(self, command):
        """
        _executeCommand_

        Util it execute the command provided in a popen object

        """
        

        child = popen2.Popen3(command, 1) # capture stdout and stderr from command
        child.tochild.close()             # don't need to talk to child
        outfile = child.fromchild 
        outfd = outfile.fileno()
        errfile = child.childerr
        errfd = errfile.fileno()
        makeNonBlocking(outfd)            # don't deadlock!
        makeNonBlocking(errfd)
        outdata = errdata = ''
        outeof = erreof = 0
        stdoutBuffer = ""
        while 1:
            ready = select.select([outfd,errfd],[],[]) # wait for input
            if outfd in ready[0]:
                outchunk = outfile.read()
                if outchunk == '': outeof = 1
                stdoutBuffer += outchunk
                sys.stdout.write(outchunk)
            if errfd in ready[0]:
                errchunk = errfile.read()
                if errchunk == '': erreof = 1
                sys.stderr.write(errchunk)
            if outeof and erreof: break
            select.select([],[],[],.1) # give a little time for buffers to fill

        try:
            exitCode = child.poll()
        except Exception, ex:
            msg = "Error retrieving child exit code: %s\n" % ex
            msg = "while executing command:\n"
            msg += command
            print msg
            return 1
        
        if exitCode:
            msg = "Error executing command:\n"
            msg += command
            msg += "Exited with code: %s\n" % exitCode
            print msg
        return exitCode
        


    def __call__(self):
        """
        _operator()_

        Invoke the Prestage

        """
        try:
            self.searchTFC()
        except Exception, ex:
            msg = "Error Converting LFNs to PFNs:\n"
            msg += str(ex)
            raise RuntimeError, msg

        try:
            self.stageInFiles()
        except Exception, ex:
            msg = "Error Prestaging Files\n"
            msg += str(ex)
            raise RuntimeError, msg

        #  //
        # // This only happens if stage in is successful
        #//
        self.generateTFC() 
           

    def searchTFC(self):
        """
        _searchTFC_

        Search the Trivial File Catalog for the lfns 
        map them to PFNs

        """       

        tfcUrl = self.siteConf.eventData['catalog']
       
        tfcInstance = loadTFC (tfcUrl)
        tfcProto = tfcInstance.preferredProtocol 


        for lfn in self.files:
            pfn = tfcInstance.matchLFN(tfcProto, lfn)
            if pfn == None:
                msg = "Unable to map LFN to PFN:\n"
                msg += "LFN: %s\n" % lfn
                continue

            msg = "LFN to PFN match made:\n"
            msg += "LFN: %s\nPFN: %s\n" % (lfn, pfn)
            print msg
            if pfn.startswith('rfio:'):
               pfn = pfn.split(':')[1]   
            self.lfnToPfn[lfn] = pfn
        return
        
    def stageInFiles(self):
        """
        _stageInFiles_

        rfcp in the PFNs

        """
        workingDir = os.path.join(os.getcwd(), "prestage")
        if not os.path.exists(workingDir):
            os.makedirs(workingDir)

        for i in range(self.numberOfRetries):
            
            for lfn, pfn in self.lfnToPfn.items():
                localPfn = "%s/%s" % (workingDir, os.path.basename(lfn))
                command = "rfcp %s %s" % (pfn, localPfn)
                #  //
                # // TODO: Call out and execute the command
                #//  Catch errors 
                print "Executing Stage In:"
                print command
                exitCode = self.executeCommand(command)
             
                if exitCode != 0:                    
                    continue

                #  //
                # // Existence check of staged in files
                #//
                if not os.path.exists(localPfn):
                    continue              


                self.localFiles[lfn] = localPfn                 
                del self.lfnToPfn[lfn]

            if len(self.lfnToPfn) == 0:
               break

            else:
               time.sleep(self.retryInterval)

        if len(self.lfnToPfn.keys()) > 0:
            msg = "Failed to stage in files:\n"
            for k,v in self.lfnToPfn.items():
                msg += " %s %s \n" % k,v
            print msg
            raise RuntimeError, msg

        return

    def generateTFC(self):
        """
        _generateTFC_

        Method to generate on the fly TFC. Each LFN would be mapped to local PFN 
        """ 
        workingDir = os.path.join(os.getcwd(),'prestage')
        if not os.path.exists(workingDir):
           os.mkdirs(workingDir)
        
        tfcFile = IMProvDoc ('storage-mapping')

        for item in self.localFiles.keys():

           if item.startswith('/'):
              temp = item.split('/',1)[1]

           temp = os.path.split(temp) 

           params = {'protocol':'local-stage-in','path-match':'/%s/(%s)' % (temp[0],temp[1]),'result':'file:%s/$1' % workingDir}

           node = IMProvNode('lfn-to-pfn',None,**params)
           tfcFile.addNode(node)
        
        handle = open('%s/prestageTFC.xml' % workingDir, 'w')
        handle.write (tfcFile.makeDOMDocument().toprettyxml() )
        handle.close()           
       
        return

 

        

if __name__ == '__main__':
    prestager = Prestager()
    prestager()
    
