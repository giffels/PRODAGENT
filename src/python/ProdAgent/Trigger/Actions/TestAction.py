
from ProdAgent.Trigger.Actions.ActionInterface import ActionInterface
from ProdCommon.Core.GlobalRegistry import registerHandler


class TestAction(ActionInterface):

   def __init__(self):
      ActionInterface.__init__(self)
      self.args={}

   def invoke(self,jobSpecId):
      self.messageService.publish("testEvent","none")
      print('-->Action Test Action is being Invoked for jobSpecID: '+jobSpecId)
      self.messageService.commit()

registerHandler(TestAction(),"testAction","ProdAgent.Triggers")
