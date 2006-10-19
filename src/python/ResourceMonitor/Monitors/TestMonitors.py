#!/usr/bin/env python
"""
_TestMonitors_

A couple of "fake" monitor plugins that act like they are checking a batch
system when they arent.

RandomMonitor - Narrow Gaussian random number, mean of zero
FixedMonitor - Always returns a fixed value

WARNING: If used without care these plugins could well bomb a batch system
into the ground.

"""
from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor



import random

#  //
# // Parameters used for tests: Should come from the cfg...
#//
_FixedValue = 0
_GaussMean = 0
_GaussStdDev = 2

class FixedMonitor(MonitorInterface):
    """
    _FixedMonitor_

    Every time this plugin is polled, return the same value

    """
    def __call__(self):
        """
        _operator()_

        Fake callout to check a batch system
        """
        constraint = self.newConstraint()
        constraint['count'] = _FixedValue
        return constraint





class RandomMonitor(MonitorInterface):
    """
    _RandomMonitor_

    Randomly generate 0-few jobs per call for testing
    purposes

    """
    def __call__(self):
        """
        _operator()_

        Fake callout to check a batch system
        """
        constraint = self.newConstraint()
        constraint['count'] = int(abs(random.gauss(_GaussMean, _GaussStdDev)))
        return constraint







registerMonitor(FixedMonitor, FixedMonitor.__name__)
registerMonitor(RandomMonitor, RandomMonitor.__name__)
