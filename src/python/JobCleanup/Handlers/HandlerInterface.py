#!/usr/bin/env python


class HandlerInterface:
    """
    _HandlerInterface_

    Common Hander Interface, Handler implementations should inherit 
    from this class and implement the handleError method.
   
    """

    def __init__(self):
         """

         Constructor

         """
         self.parameters ={}

    def handleEvent(self,payload):
         """
         _handleError_
         
         Handles the error based on the payload it receives.
         """

         msg = "Virtual Method HandlerInterface.handleError called"
         raise RuntimeError, msg

    def __call__(self,payload):
         """
         Call method
         """
         self.handleError()
