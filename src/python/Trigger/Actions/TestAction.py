
from Trigger.Registry import registerAction
from Trigger.Actions.ActionInterface import ActionInterface


class TestAction(ActionInterface):

   def __init__(self):
      ActionInterface.__init__(self)
      self.args={}

   def invoke(self):
      print('-->Action Test Action is being Invoked')

registerAction(TestAction(),"testAction")
