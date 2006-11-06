#!/usr/bin/env python
"""
_workflowSummary_

Extract details from StatTracker DB and produce a summary for a given workflow

"""


import sys
import getopt

valid = ['workflows-matching=', 'interval=', 'workflow=']

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print str(ex)
    sys.exit(1)

workflowMatch = None
workflowName = None
interval = "24:00:00"


for opt, arg in opts:
    if opt == "--workflows-matching":
        workflowMatch = arg
    if opt == "--interval":
        interval = arg
    if opt == "--workflow":
        workflowName = arg


if workflowMatch == None:
    workflowMatch = ""




import StatTracker.StatTrackerAPI as Stats


if workflowName != None:
    print "Workflow:\t  %s " % workflowName
    print Stats.shortTextWorkflowSummary(workflowName)
    sys.exit(0)

allWorkflows = Stats.workflowSpecs() # all workflows known to StatTracker DB
for wf in allWorkflows:
    #  //
    # // filter based on command line match
    #//
    if wf.startswith(workflowMatch):
        print "Workflow:\t  %s " % wf
        print Stats.shortTextWorkflowSummary(wf)




#
#    print

sys.exit(0)



    
