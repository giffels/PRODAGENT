#!/usr/bin/env python
"""
_T0LSFSubmitter_

Submitter for T0 LSF submissions that is capable of handling both
Bulk and single LSF submissions


"""
import os
import logging

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException


class T0LSFSubmitter(BulkSubmitterInterface):
    """
    _T0LSFSubmitter_

    Submitter Plugin to submit jobs to the Tier-0 LSF system.

    Can generate bulk or single type submissions.

    """
    def doSubmit(self):
        """
        _doSubmit_

        Main method to generate job submission.

        Attributes in Base Class BulkSubmitterInterface are populated with
        details of what to submit in terms of the job spec and
        details contained therein

        """
        logging.debug("<<<<<<<<<<<<T0LSFSubmitter>>>>>>>>>>")

        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        self.mainSandbox = \
                   self.primarySpecInstance.parameters['BulkInputSandbox']
        self.mainSandboxName = os.path.basename(self.mainSandbox)
        self.specSandboxName = None
        self.singleSpecName = None
        #  //
        # // Build a list of input files for every job
        #//
        self.jobInputFiles = []
        self.jobInputFiles.append(self.mainSandbox)
        
        #  //
        # // For multiple bulk jobs there will be a tar of specs
        #//
        if self.primarySpecInstance.parameters.has_key('BulkInputSpecSandbox'):
            self.specSandboxName = os.path.basename(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
                )
            self.jobInputFiles.append(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox'])

        #  //
        # // For single jobs there will be just one job spec
        #//
        if not self.isBulk:
            self.jobInputFiles.append(self.specFiles[self.mainJobSpecName])
            self.singleSpecName = os.path.basename(
                self.specFiles[self.mainJobSpecName])

        
        #  // So now we have a list of input files for each job
        # //  which have to be available to the job at runtime.
        #//   So we may need some copy operation here to a drop
        #  // box area visible on the WNs etc.
        # //
        #//

        #  //
        # // We have a list of job IDs to submit.
        #//  If this is a single job, there will be just one entry
        #  //If it is a bulk submit, there will be multiple entries,
        # // plus self.isBulk will be True
        #//
        submitList = self.toSubmit.keys()

        #  //
        # // This is as far as I have got so far, but I have some ideas...
        #//


#  //
# // BRAINSTORMING:
#//
#
#  For bulk submission, we compile a list of run numbers
#  These can be used as the Job Array for bulk submission
#   -J "WorkflowSpecID-[minRun-maxRun]"
#
#  In the exe script we generate and submit, the job spec
#  ID & file can be constructed using the $LSB_JOBINDEX which
#  will be the run number. The JobSpec file to use will then
#  be WorkflowSpecID-$LSB_JOBINDEX which means we can find the
#  file in the spec tarball.
#
#  We need to use a group to make tracking easy:       
#  -g /groups/tier0/reconstruction
#        
#        
#  The Job needs to run and drop off the FrameworkJobReport somewhere  
#  ultimately this needs to be the JobCreator cache dir for the JobSpecID
#  but an intermediate drop box and migration by the tracking component would
#  work.
#
#  Logfiles probably should be redirected to the Job Cache area as well
#
#  We need to turn off the bloody emails.
#
#
#
#

registerSubmitter(T0LSFSubmitter, "Tier0LSF")
