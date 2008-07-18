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

    def handleError(self,payload):
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


    def reSubmit(self, jobspecfile, generalInfo):
        """
        Either resubmit job or add back to jobQueue
        """
        if self.parameters.get("ReQueueFailures", False):
            pass
        else:
            # a submit event with delay
            delay = int(self.args['DelayFactor']) * \
                            (int(generalInfo['Retries'] + 1))
            delay = convertSeconds(delay) 
            logging.debug(">RunFailureHandler<: re-submitting with delay (h:m:s) "+\
                          str(delay))
            self.publishEvent("SubmitJob",jobspecfile,delay)
         