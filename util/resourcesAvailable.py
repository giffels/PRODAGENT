#!/usr/bin/env python
"""
_resourcesAvailable_

Util for publishing a constrained resources available event

"""

import sys
import getopt


from MessageService.MessageService import MessageService
from ProdAgentCore.ResourceConstraint import ResourceConstraint


valid = ['njobs=', 'sites=', 'workflow=', 'type=']

usage = "Usage: resourcesAvailable.py --njobs=<Number Of Jobs>/n"
usage += "                             --workflow=<Optional Constraint>\n"
usage += "                             --sites=<Optional Constraint>\n"
usage += "                             --type=<Optional Constraint>\n"



try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

numJobs = 1
sites = None
workflow = None
jobType = None


for opt, arg in opts:
    if opt == "--njobs":
        numJobs = arg
    if opt == "--sites":
        sites = arg
    if opt == "--workflow":
        workflow = arg
    if opt == "--type":
        jobType = arg



constraint = ResourceConstraint()
constraint["count"] = numJobs
constraint["type"] = jobType
constraint["workflow"] = workflow
constraint["site"] = sites

ms = MessageService()
ms.registerAs("CLI")
ms.publish("ResourcesAvailable", str(constraint))
ms.commit()
