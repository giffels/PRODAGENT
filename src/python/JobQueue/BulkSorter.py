#!/usr/bin/env python
"""
_BulkSorter_

Object to sort a list of jobs into individual and bulk submission groups

Algorithm is as follows:

Given a list of jobs:

1. all merge type jobs are individual
2. all processing jobs are sorted into a hash table where the hash
   is workflow-site
3. Each job is added to a list under the appropriate hash
4. Hash Lists containing only one element are added to individual jobs
5. Hash lists containing multiple elements are kept as bulk lists


"""


class BulkSorter:
    """
    _BulkSorter_

    Given a list of job specs and parameters, sort them into
    indivdiual or bulk spec groupings

    """
    def __init__(self):
        self.individualSpecs = []
        self.bulkSpecs = {}


    def __call__(self, *jobs):
        """
        _operator(jobs)_

        Sort list of jobs

        """
 
        #  //
        # //  Sort jobs into potential bulk specs
        #//
        for job in jobs:
        #    if job['JobType'] in ('Merge', 'CleanUp', 'LogCollect'):
            if job['JobType'] in ('CleanUp', 'LogCollect'):
                #  //
                # // all merges and cleanups are individual
                #//
                self.individualSpecs.append(job)
                continue
            #  //
            # // All others are stored by hash of workflow and site
            #//
            hashKey = "%s-%s" % (job['WorkflowSpecId'], job['Site'])
            if not self.bulkSpecs.has_key(hashKey):
                self.bulkSpecs[hashKey] = []
            self.bulkSpecs[hashKey].append(job)

        #  // 
        # // Prune out 1 element bulk entries and add them to
        #//  individual list
        for key, specList in self.bulkSpecs.items():
            if len(specList) == 1:
                self.individualSpecs.append(specList[0])
                del self.bulkSpecs[key]
        return
    

        
