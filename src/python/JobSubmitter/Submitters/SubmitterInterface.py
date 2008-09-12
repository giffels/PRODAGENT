#!/usr/bin/env python
"""
_SubmitterInterface_

Common Submitter Interface, Submitter implementations should inherit this
class.

Submitters should not take any ctor args since they will be instantiated
by a factory

"""
import os
import logging
from popen2 import Popen4



class SubmitterInterface:
    """
    _SubmitterInterface_

    General interface for submitter implementation.
    Submitters should inherit this class and implement the
    doSubmit method.


    """
    def __init__(self):
        self.parameters = {}
    
    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_
        
        Invoke some command that submits the job.

        Arguments are the location of the wrapper script to submit the job
        with as an executable, and the location of the jobTarball that
        contains the actual job guts.
        
        """
        msg =  "Virtual Method SubmitterInterface.doSubmit called"
        raise RuntimeError, msg
        
    
    def generateWrapper(self, wrapperName, tarball, jobname):
        """
        _generateWrapper_

        Generate an execution wrapper for the job. Arguments provided
        are the name of the job Tarball and the name of the executable
        within that tarball.

        - *wrapperName* : Location of the script, create this file

        - *tarball* : Complete path to the tarball in its
        presubmission location

        - *jobname* : Name of the job. this is also the name of the top
        level dir in the tarfile where the job should be executed from.

        The main script for the job is always <jobname>/run.sh and you
        should invoke run.sh from the directory in which it resides

        This base class provides a very simple wrapper generation mechanism
        that builds a bash script
        
        """
        script = ["#!/bin/sh\n"]
        script.append("tar -zxf %s\n" % os.path.basename(tarball))
        script.append("cd %s\n" % jobname)
        script.append("./run.sh\n")
        
        handle = open(wrapperName, 'w')
        handle.writelines(script)
        handle.close()

        return 
        

    def __call__(self, workingDir, jobCreationArea, jobname, **args):
        """
        _Operator()_

        Take a working area, job area and job name and generate
        a tarball in the working area containing the jobCreationArea
        contents.
        
        Invoke the overloaded methods to handle wrapper creation and
        submission for that job

        """
        logging.debug("SubmitterInterface.__call__")
        logging.debug("Subclass:%s" % self.__class__.__name__)
        self.parameters.update(args)

        #  //
        # // Check to see Job tarball exists, if not create it, if
        #//  so, then check to see if it needs recreating
        tarballName = self.tarballName(workingDir, jobname)
        tarballExists = os.path.exists(tarballName)
        if not tarballExists:
            tarball = createTarball(workingDir, jobCreationArea, jobname)
        else:
            #  //
            # // Tarball exists: Do we need to recreate it?
            #//
            if not self.parameters.get("NoRecreate", False):
                #  //
                # // NoRecreate is False, so we recreate 
                #//
                tarball = createTarball(workingDir, jobCreationArea, jobname)
                
        wrapperName = os.path.join(workingDir, "%s-submit" % jobname)
        logging.debug("SubmitterInterface:Tarball=%s" % tarball)
        logging.debug("SubmitterInterface:Wrapper=%s" % wrapperName)

        #  //
        # // save file and directory information for subclasses use (Carlos)
        #//
        self.baseDir = os.path.dirname(os.path.dirname(jobCreationArea))
        self.executableFile = "%s-submit" % os.path.dirname(jobCreationArea)

        #  //
        # // Generate some standard parameters
        #//
        self.parameters['BaseDir'] = os.path.dirname(
            os.path.dirname(jobCreationArea)
            )
        self.parameters['ExecutableFile'] = "%s-submit" % (
            os.path.dirname(jobCreationArea),
            )
        self.parameters['JobCacheArea'] = jobCreationArea
        self.parameters['JobName'] = jobname
        self.parameters['Tarball'] = tarballName
        self.parameters['Wrapper'] = wrapperName
        self.parameters['AppVersions'] = \
                   self.parameters['JobSpecInstance'].listApplicationVersions()
        bossId = self.isBOSSDeclared()
        if bossId != None:
            self.parameters['BOSSID'] = bossId
            

        #  //
        # // Generate a wrapper script
        #//
        self.generateWrapper(wrapperName, tarball, jobname)

        #  //
        # // Invoke whatever is needed to do the submission
        #//
        self.doSubmit(wrapperName, tarball)
        
        return

    def executeCommand(self, command):
        """
        _executeCommand_

        Util it execute the command provided in a popen object

        """
        logging.debug("SubmitterInterface.executeCommand:%s" % command)
        pop = Popen4(command)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()

        if exitCode:
            msg = "Error executing command:\n"
            msg += command
            msg += "Exited with code: %s\n" % exitCode
            msg += pop.fromchild.read()
            logging.error("SubmitterInterface:Failed to Execute Command")
            logging.error(msg)
            raise RuntimeError, msg
        return pop.fromchild.read()
    

    def tarballName(self, targetDir, jobName):
        """
        _tarballName_

        What is the name of the tarball for this job being submitted,
        including path?

        """
        return os.path.join(targetDir, "%s.tar.gz" % jobName)

    def declareToBOSS(self):
        """
        _declareToBOSS_

        Declare this job to BOSS.
        Parameters are extracted from this instance

        """
        logging.debug("SubmitterInterface:Declaring Job To BOSS")
        if not os.environ["BOSSVERSION"]=="v4_0_0":
            bossQuery = "boss SQL -query \"select name from JOBTYPE "
            bossQuery += "where name = 'cmssw'\""
            queryOut = self.executeCommand(bossQuery)
            bossJobType = "cmssw"
            if queryOut.find("cmssw") < 0:
                bossJobType="stdjob"

        #  //
        # // Write classad file for boss declare in the job Cache area
        #//
        cladfile = "%s/%s.clad" % (
            self.parameters['JobCacheArea'], self.parameters['JobName'],
            )
        declareClad=open(cladfile,"w")
        declareClad.write("executable = %s;\n" % (
            self.parameters['ExecutableFile'],
            )
                          )
        declareClad.write("jobtype = %s;\n" % bossJobType)
        declareClad.write("stdout = %s.stdout;\n" % self.parameters['JobName'])
        declareClad.write("stderr = %s.stderr;\n"% self.parameters['JobName'])
        declareClad.write("infiles = %s,%s;\n" % (
            self.parameters['Wrapper'], self.parameters['Tarball'],
            )
                          )
        outfiles = "outfiles = %s.stdout,%s.stderr," % (
            self.parameters['JobName'], self.parameters['JobName'],
            )
        outfiles += "FrameworkJobReport.xml;\n" 
        declareClad.write(outfiles)
        declareClad.close()
        logging.debug("SubmitterInterface:BOSS Classad written:%s" % cladfile)

        #  //
        # // Do BOSS Declare
        #//
        bossDeclare = "boss declare -classad %s " % cladfile
        bossJobId = self.executeCommand(bossDeclare)
        logging.debug("SubmitterInterface:BOSS Job ID: %s" % bossJobId)

        #  //
        # // Write ID to cache
        #//
        idFile = "%s/%sid" % (
            self.parameters['JobCacheArea'], self.parameters['JobName'],
            )
        handle = open(idFile, 'w')
        handle.write("JobId=%s" % bossJobId)
        handle.close()
        logging.debug("SubmitterInterface:BOSS JobID File:%s" % idFile)
        os.remove(cladfile)
        return

    def isBOSSDeclared(self):
        """
        _isBOSSDeclared_

        If this job has been declared to BOSS, return the BOSS ID
        from the cache area. If it has not, return None

        """
        idFile = "%s/%sid" % (
            self.parameters['JobCacheArea'], self.parameters['JobName'],
            )
        if not os.path.exists(idFile):
            #  //
            # // No BOSS Id File ==> not declared
            #//
            return None
        content = file(idFile).read().strip()
        content.replace("JobId=", "")
        try:
            jobId = int(content)
        except ValueError:
            jobId = None
        return jobId
        

        
    

def createTarball(targetDir, sourceDir, tarballName):
    """
    _createTarball_

    Create a tarball in targetDir named tarballName.tar.gz containing
    the contents of sourceDir.

    Return the path to the resulting tarball

    """
    logging.debug("SubmitterInterface.createTarball")
    logging.debug("createTarball:Target=%s" % targetDir)
    logging.debug("createTarball:Source=%s" % sourceDir)
    logging.debug("createTarball:Tarball=%s" % tarballName)
    tarballFile = os.path.join(targetDir, "%s.tar.gz" % tarballName)
    if os.path.exists(tarballFile):
        logging.debug(
            "createTarball:Tarball exists, cleaning: %s" % tarballFile)
        os.remove(tarballFile)
    
    tarComm = "tar -czf %s -C %s %s " % (
        tarballFile,
        os.path.dirname(sourceDir),
        os.path.basename(sourceDir)
        )

    pop = Popen4(tarComm)
    while pop.poll() == -1:
            exitCode = pop.poll()
    exitCode = pop.poll()

    if exitCode:
        msg = "Error creating Tarfile:\n"
        msg += tarComm
        msg += "Exited with code: %s\n" % exitCode
        msg += pop.fromchild.read()
        logging.error("createTarball: Tarball creation failed:")
        logging.error(msg)
        raise RuntimeError, msg
    return tarballFile

    
    
    
