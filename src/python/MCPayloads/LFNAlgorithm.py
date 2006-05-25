#!/usr/bin/env python
"""
_LFNAlgorithm_

Algorithmic generation of Logical File Names using the CMS LFN Convention

"""
__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "evansde@fnal.gov"

import time

#  //
# // All LFNS start with this constant value
#//
_LFNBase = "/store"


def makeTimestampString(timestamp):
    """
    _makeTimestampString_

    Working from an integer timestamp, generate the datestamp fragment
    of the LFN.

    This uses gmtime as a convention so that all LFNs get the same
    timestamp based on the request, rather than the local time zone
    interpretation.

    Why GMT?  Because Im British dammit! God Save The Queen!

    """
    gmTuple = time.gmtime(timestamp)
    year = gmTuple[0]
    month = gmTuple[1]
    day = gmTuple[2]

    return "%s/%s/%s" % (year, month, day)



def unmergedLFNBase(workflowSpecInstance):
    """
    _unmergedLFNBase_

    Generate the base name for LFNs for the WorkflowSpec Instance
    provided

    """
    category = workflowSpecInstance.requestCategory()
    result = os.path.join(_LFNBase, category, "unmerged")
    timestamp = workflowSpecInstance.requestTimestamp()
    result = os.path.join(
        result,
        makeTimestampString(timestamp),      # time/date
        workflowSpecInstance.workflowName()  # name of workflow/request
        )
    #  //
    # // Add this to the WorkflowSpec instance
    #//
    workflowSpecInstance.parameters['UnmergedLFNBase'] = result
    return result

def mergedLFNBase(workflowSpecInstance):
    """
    _mergedLFNBase_

    Generate the base name for LFNs for the WorkflowSpec Instance provided
    for the output of merge jobs.

    """
    category = workflowSpecInstance.requestCategory()
    result = os.path.join(_LFNBase, category)
    timestamp = workflowSpecInstance.requestTimestamp()
    result = os.path.join(
        result,
        makeTimestampString(timestamp),      # time/date
        workflowSpecInstance.workflowName()  # name of workflow/request
        )
    #  //
    # // Add this to the WorkflowSpec instance
    #//
    workflowSpecInstance.parameters['MergedLFNBase'] = result
    return result
    

def generateLFN(requestBase, lfnGroup, jobName, dataTier):
    """
    _generateLFN_

    Create the LFN using:

    - *requestBase* output of merged or unmergedLFNBase method for
                    workflow spec

    - *lfnGroup* integer counter used to partition lfn namespace

    - *jobName* The JobSpec ID

    - *dataTier* The Data Tier of the file being produced
                 (usually same as output module name)

    """
    result = os.path.join(requestBase, lfnGroup)                    
    result += "/"
    result += jobName
    result += "-"
    result += dataTier
    result += ".root"
    return result



def createJobSpecNodeLFNs(self, jobSpecNode):
    """
    _createJobSpecNodeLFNs_

    For each output module in the jobSpecNode instance provided,
    generate an LFN.

    Return dict of output module name to lfn

    """
    pass

