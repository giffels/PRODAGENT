#!/usr/bin/env python


class ActionInterface:
    """
    _ActionInterface_

    Common Action Interface, Action implementations should inherit 
    from this class and implement the invoke method.
   
    """

    def __init__(self):
         """

         Constructor

         """
         self.parameters ={}

    def invoke(self,jobSpecId):
         """
         _invoke_
        
         invoke the action. 
         """

         msg = "Virtual Method ActionInterface.invoke called"
         raise RuntimeError, msg

    def __call__(self,jobSpecId):
         """
         Call method
         """
         self.invoke(jobSpecId)
