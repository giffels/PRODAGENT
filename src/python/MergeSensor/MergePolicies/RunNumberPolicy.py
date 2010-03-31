#!/usr/bin/env python
"""
_RunNumberPolicy_

Policy for merge based on run numbers

"""

__revision__ = "$Id: RunNumberPolicy.py,v 1.3 2008/10/20 12:48:45 swakef Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import logging

from MergeSensor.Registry import registerMergePolicy

class RunNumberPolicy:
    """
    _RunNumberPolicy_

    Implements a merge policy based on the run number

    """

    ##########################################################################
    # initialization
    ##########################################################################

    def __init__(self):
        """
        not used
        """

        pass

    ##########################################################################
    # apply the selection policy defined by the plugin
    ##########################################################################

    def applySelectionPolicy(self, fileList, parameters, forceMerge = False):
        """
        _applySelectionPolicy_

        Apply the selection policy to the set of files passed as argument,
        returning the selected set plus the file block id.

        Arguments:

          fileList -- the files
          parameters -- a dictionary with parameters
          forceMerge -- True indicates that a set should be selected even
                        if the conditions do not apply.

        Return:

          the list of selected files and its block id.

        """

        logging.debug("RunNumber Policy.applySelectionPolicy(%s,%s,%s)" % \
                     (fileList, parameters, forceMerge))

        # get parameters
        maxMergeFileSize = parameters['maxMergeFileSize']
        minMergeFileSize = parameters['minMergeFileSize']
        #mergeByFileCount = parameters.get('mergeByFileCount', False)
        filesPerMergeJob = parameters['filesPerMergeJob']

        # check all file blocks in dataset
        for fileBlock in fileList:

            # get data
            fileBlockId, files = fileBlock

            # get run numbers
            try:
                runs = set([f['run'][0]  for f in files])

                # organize files by runs
                run = {}
                for r in runs:
                    run[r] = [f for f in files if f['run'][0] == r]
            except Exception, msg:
                logging.error("Problems getting run numbers: %s" % str(msg))
                return ([], 0)

            # check for all run numbers
            for files in run.values():
 
                # select set of files with at least mergeFileSize size
                # or select the number of files set by filesPerMergeJob
                # in any case the merge size has to be less than
                # maxMergeFileSize
                totalSize = 0
                fileCounter = 0
                selectedSet = []
                numFiles = len(files)

                # start with the longest file
                startingFile = 0

                # ignore too large files in force merge
                tooLargeFiles = 0

                # try to start filling a bin
                while startingFile < numFiles:

                    selectedSet = [files[startingFile]['name']]
                    totalSize = files[startingFile]['filesize']
                    leftIndex = startingFile + 1
                    fileCounter = 1

                    # verify that the file is not larger that maximum
                    if totalSize > maxMergeFileSize:
                        logging.warning( \
                                    "File %s is too big, will not be merged" \
                                    % files[startingFile]['name'])
                        startingFile = startingFile + 1
                        tooLargeFiles = tooLargeFiles + 1
                        continue

                    # continue filling it
                    while totalSize < minMergeFileSize and \
                        totalSize < maxMergeFileSize and \
                        leftIndex < numFiles and \
                        (filesPerMergeJob < 1 or \
                            fileCounter < filesPerMergeJob):

                        # attempt to add other file
                        newSize = totalSize + files[leftIndex]['filesize']
  
                        # check if we have not gone over maximum
                        if newSize < maxMergeFileSize:

                            # great, add it
                            selectedSet.append(files[leftIndex]['name'])
                            totalSize = newSize

                        # still space, try to add the next one
                        leftIndex = leftIndex + 1
                        fileCounter = fileCounter + 1

                    # verify results
                    if ((filesPerMergeJob > 0 and \
                                fileCounter >= filesPerMergeJob) or \
                            totalSize >= minMergeFileSize) and \
                        totalSize < maxMergeFileSize:

                        # done
                        logging.info("Merge Job will be created.")
                        logging.info("Number of files: %s" % fileCounter)
                        logging.info("Total Size: %s" % totalSize)
                        return (selectedSet, fileBlockId)
 
                    # try starting bin from second file
                    startingFile = startingFile + 1

                # not enough files, continue to next run
                # if forceMerge and list non-empty, return what we have
                # if forceMerge and list empty, make log entry and continue
                # with next fileBlock

                if forceMerge:

                    # get a set of files which will not go over the maximum
                    # even if the size can be smaller that minimum
                    totalSize = 0
                    selectedSet = []

                    for file in files:
                        fileName = file['name']
                        size = file['filesize']
 
                        # ignore too large files
                        if tooLargeFiles > 0:
                            tooLargeFiles = tooLargeFiles - 1
                            continue

                        # try adding a new file
                        newSize = totalSize + size

                        # verify size
                        if newSize > maxMergeFileSize:

                            # too large
                            break

                        # add it
                        selectedSet.append(fileName)
                        totalSize = totalSize + size

                    # verify if some files were selected or not
                    if selectedSet == []:
                        logging.info( \
                           "Forced merge does not apply to fileblock %s " + \
                           "due to non mergeable condition" % fileBlockId)
                        continue
                    else:

                        # ok, return them
                        logging.info("Merge Job will be created.")
                        logging.info("Number of files: %s" % len(selectedSet))
                        logging.info("Total Size: %s" % totalSize)
                        return(selectedSet, fileBlockId)
                else:

                    # no, try next run
                    continue

        # nothing to merge
        return ([], 0)

        
registerMergePolicy(RunNumberPolicy, RunNumberPolicy.__name__)

