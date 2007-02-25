
from ProdAgent.Trigger.Actions.ActionInterface import ActionInterface
from ProdCommon.Core.GlobalRegistry import registerHandler

class JobCleanAction(ActionInterface):

   """
   _JobCleanAction_

   A thin class which is associated to a trigger.
   If the trigger is "triggered" it will emit a job clean action.

   """

   def __init__(self):
      ActionInterface.__init__(self)
      self.args={}

   def invoke(self,jobSpecId):
      self.messageService.publish("JobCleanup",jobSpecId)
      self.messageService.commit()

registerHandler(JobCleanAction(),"jobCleanAction","ProdAgent.Triggers")
