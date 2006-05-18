from MessageService.MessageService import MessageService
import os
import xmlrpclib

print "Make sure the RequestInjector, JobCreator,"
print "JobSubmitter and ErrorHandler are running"
# we use this for event publication.
ms=MessageService()
ms.registerAs("TestComponent")
# subscribe on the events this or other test produce
# so we can verify this in the database


# Request Injector generated
#ms.subscribeTo("CreateJob")
# Job Creator generated
#ms.subscribeTo("SubmitJob")
# Job Submitter generated
ms.subscribeTo("JobSubmitted")
ms.subscribeTo("SubmissionFailed")
# Job Tracker generated
#ms.subscribeTo("JobFailed")
#ms.subscribeTo("JobSuccess")

# Error Handler generated
ms.subscribeTo("SubmissionFailed")
ms.subscribeTo("JobSubmitted")
ms.subscribeTo("GeneralJobFailure")

# set the data directory.
thisPath=os.getcwd()
chunkIndex=thisPath.rfind('test/python')
dataPath=thisPath[0:chunkIndex]+'test/data/input/'


# publish events for first components. 
ms.publish("RequestInjector:StartDebug","none")
ms.publish("JobCreator:StartDebug","none")
ms.publish("JobSubmitter:StartDebug","none")
ms.publish("ErrorHandler:StartDebug","none")
ms.publish("JobCleanup:StartDebug","none")
#ms.publish("JobSubmitter:SetSubmitter","boss")
#ms.publish("TrackingComponent:StartDebug","none")

ms.publish("RequestInjector:SetWorkflow",dataPath+\
                 'workflowSpec/PreProdR2Electron300GeV-Workflow.xml')
ms.publish("RequestInjector:SetEventsPerJob",str(100))
ms.publish("ResourcesAvailable", "none")

ms.publish("RequestInjector:SetWorkflow",dataPath+\
                'workflowSpec/PreProdR2Electron10GeV-Workflow.xml')
ms.publish("RequestInjector:SetEventsPerJob",str(100))
ms.publish("ResourcesAvailable", "none")

ms.publish("RequestInjector:SetWorkflow",dataPath+\
                 'workflowSpec/PreProdR2Mu10GeV-Workflow.xml')
ms.publish("RequestInjector:SetEventsPerJob",str(100))
ms.publish("ResourcesAvailable", "none")

ms.publish("RequestInjector:SetWorkflow",dataPath+\
                 'workflowSpec/PreProdR2Pion300GeV-Workflow.xml')
ms.publish("RequestInjector:SetEventsPerJob",str(100))
ms.publish("ResourcesAvailable", "none")

ms.publish("RequestInjector:SetWorkflow",dataPath+\
                 'workflowSpec/PreProdR2Mu300GeV-Workflow.xml')
ms.publish("RequestInjector:SetEventsPerJob",str(100))
ms.publish("ResourcesAvailable", "none")

ms.publish("RequestInjector:SetWorkflow",dataPath+\
                 'workflowSpec/PreProdR2Minbias-Workflow.xml')
ms.publish("RequestInjector:SetEventsPerJob",str(100))
ms.publish("ResourcesAvailable", "none")

ms.publish("RequestInjector:SetWorkflow",dataPath+\
                 'workflowSpec/PreProdR2Pion10GeV-Workflow.xml')
ms.publish("RequestInjector:SetEventsPerJob",str(100))
ms.publish("ResourcesAvailable", "none")
ms.commit()
