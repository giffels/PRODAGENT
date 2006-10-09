#!/usr/bin/env python


class StateInterface:
    """
    _StateInterface_

    Common State Interface, State implementations should inherit 
    from this class and implement the execute method.
   
    """

    def __init__(self):
         """

         Constructor

         """

    def execute(self):
         """
         _execute_
         
         Handles the error based on the payload it receives.
         """

         msg = "Virtual Method StateInterface.execute called"
         raise RuntimeError, msg

    def __call__(self):
         """
         Call method
         """
