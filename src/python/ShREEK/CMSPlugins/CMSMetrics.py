#!/usr/bin/env python
"""
_CMSMetrics_

Updator Plugins for ShREEK to generate metrics in the Monitor State

These objects should be callable, and return some value.
When registered as a plugin, the monitor state will periodically invoke
the call and the monitor state field will be set to that value

An updator must take a single argument to its call: that argument will
be the monitor state instance.

"""

from ShREEK.ShREEKPluginMgr import registerShREEKUpdator


#  //
# // Define the updator
#//
def exampleUpdator(state):
    print " >>> exampleUpdator"
    print " >>> state[\'Example\'] = ", state.get('Example', None)
    presentValue = state.get('Example', None)
    if presentValue in ( None, "NotUpdated"):
        return 1
    else:
        return presentValue + 1



#  //
# // register the updator
#//
registerShREEKUpdator(exampleUpdator, "Example")
