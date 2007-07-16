

from ProdCommon.MCPayloads.JobSpec import JobSpec
import JobState.JobStateAPI.JobStateChangeAPI as JobStates



import sys
from MessageService.MessageService import MessageService
ms = MessageService()
ms.registerAs("Test")

"""
Usage:  cleanAndRetry.py /path/to/JobSpec.xml
"""

specFile = sys.argv[1]

spec = JobSpec()
spec.load(specFile)


specIds = []

if spec.isBulkSpec():
    specIds.extend(spec.bulkSpecs.keys())

else:
    specIds.append(spec.parameters['JobName'])

#print specIds;

for specId in specIds:
    JobStates.cleanout(specId)
    

ms.publish("CreateJob", specFile)
ms.commit()
