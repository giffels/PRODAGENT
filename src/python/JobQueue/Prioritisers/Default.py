#!/usr/bin/env python
"""
_Default_

Default prioritisation algorithm.

Works as follows:

1. Given a constraint, use contraint values to query DB for matching
jobs.

2. releases all matches to be created

3. Ignores any undershoot in resources.

"""

import logging
from JobQueue.Prioritisers.PrioritiserInterface import PrioritiserInterface
from JobQueue.Registry import registerPrioritiser



class Default(PrioritiserInterface):
    """
    _Default_

    Returns exactly what matches the constraint
    
    """
    def __init__(self):
        PrioritiserInterface.__init__(self)


    def prioritise(self, constraint):
        """
        _prioritise_

        Get jobs from DB matching constraint

        """
        return self.matchedJobs


registerPrioritiser(Default, Default.__name__)


