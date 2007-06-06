#!/usr/bin/env python
"""
_SizePolicy_

Policy for merge based on file size

"""

__revision__ = "$Id: SizePolicy.py,v 1.1 2007/06/04 12:46:00 ckavka Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import logging

from MergeSensor.Registry import registerMergePolicy

class SizePolicy:
    """
    _SizePolicy_

    Implements a merge policy based on file size

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

        logging.debug("SizePolicy.applySelectionPolicy(%s,%s,%s)" % \
                     (fileList, parameters, forceMerge))

        # get parameters
        maxMergeFileSize = parameters['maxMergeFileSize']
        minMergeFileSize = parameters['minMergeFileSize']

        # check all file blocks in dataset
        for fileBlock in fileList:

            # get data
            fileBlockId, files = fileBlock

            # select set of files with at least mergeFileSize size
            totalSize = 0
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

                # verify that the file is not larger that maximum
                if totalSize > maxMergeFileSize:
                    self.logging.warning( \
                                    "File %s is too big, will not be merged" \
                                    % files[startingFile]['name'])
                    startingFile = startingFile + 1
                    tooLargeFiles = tooLargeFiles + 1
                    continue

                # continue filling it
                while totalSize < minMergeFileSize and \
                      totalSize < maxMergeFileSize and \
                      leftIndex < numFiles:

                    # attempt to add other file
                    newSize = totalSize + files[leftIndex]['filesize']

                    # check if we have not gone over maximum
                    if newSize < maxMergeFileSize:

                        # great, add it
                        selectedSet.append(files[leftIndex]['name'])
                        totalSize = newSize

                    # still space, try to add the next one
                    leftIndex = leftIndex + 1

                # verify results
                if totalSize >= minMergeFileSize and \
                  totalSize < maxMergeFileSize:

                    # done
                    return (selectedSet, fileBlockId)

                # try starting bin from second file
                startingFile = startingFile + 1

            # not enough files, continue to next fileBlock
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
                    self.logging.info( \
                       "Forced merge does not apply to fileblock %s " + \
                       "due to non mergeable condition" % fileBlockId)
                    continue
                else:

                    # ok, return them
                    return(selectedSet, fileBlockId)
            else:

                # no, try next file block
                continue

        # nothing to merge
        return ([], 0)

        
registerMergePolicy(SizePolicy, SizePolicy.__name__)

