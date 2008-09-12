#!/usr/bin/env python
"""
_harvestDQM_

Util for publishing a collection trigger event for a dataset/run

"""

import sys
import getopt


from MessageService.MessageService import MessageService
from DQMInjector.CollectPayload import CollectPayload



valid = ['run=', 'primary=', 'processed=', 'tier=' ]

usage = "Usage: harevstDQM.py --run=<Run Number to harvest>/n"
usage += "                    --primary=<Primary Dataset Name>\n"         
usage += "                    --processed=<Processed Dataset Name>\n"
usage += "                    --tier=<Data Tier>\n"



try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)


collect = CollectPayload()

for opt, arg in opts:
    if opt == "--run":
        collect['RunNumber'] = arg
    if opt == "--primary":
        collect['PrimaryDataset'] = arg
    if opt == "--processed":
        collect['ProcessedDataset'] = arg
    if opt == "--tier":
        collect['DataTier'] = arg


print "Publishing %s" % str(collect)

ms = MessageService()
ms.registerAs("CLI")
ms.publish("DQMInjector:Collect", str(collect))
ms.commit()


