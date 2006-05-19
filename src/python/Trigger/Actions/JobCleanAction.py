
from Trigger.Registry import registerAction
from Trigger.Actions.ActionInterface import ActionInterface


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

registerAction(JobCleanAction(),"jobCleanAction")
