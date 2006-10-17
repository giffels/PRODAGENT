#!/usr/bin/env python
"""
_MergeSensorError_

Exceptions generated by the MergeSensor

"""

__revision__ = "$Id: MergeSensorError.py,v 1.3 2006/08/25 11:03:24 ckavka Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Carlos.Kavka@ts.infn.it"

##############################################################################
# General errors
##############################################################################

class MergeSensorError(Exception):
    """ Exception object for general errors in MergeSensor component"""
    
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return repr(self.value)

##############################################################################
# Non valid datatiers
##############################################################################

class InvalidDataTier(Exception):
    """ Exception object for invalid data tier"""
    
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return repr(self.value)
        
##############################################################################
# Non valid dataset
##############################################################################

class InvalidDataset(Exception):
    """ Exception object for invalid dataset"""
    
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return repr(self.value)

##############################################################################
# Database errors
##############################################################################

class MergeSensorDBError(Exception):
    """ Exception object for DB errors"""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

##############################################################################
# Datasets that cannot be merged
##############################################################################

class NonMergeableDataset(Exception):
    """ Exception object for non mergeable datasets"""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

##############################################################################
# Dataset not in database
##############################################################################

class DatasetNotInDatabase(Exception):
    """ Exception object for dataset not in database"""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)