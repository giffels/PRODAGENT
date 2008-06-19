#!/usr/bin/env python

import logging
import os

class HandlerInterface:
      """
      _HandlerInterface_
      Interface that must be implemented by different Alert handlers
      """
      def __init__ (self):
          """
          """

          return

      def __call__ (self, payload):
          """
          """
          logging.debug('i am handler call method')
          logging.debug (payload)
          self.handleError(payload)
          return
      
      def handleError (self, payload):
          """
          """

          return     

      def publishEvent (self, name, payload, delay="00:00:00"):
          """
          """
          #raise RuntimeError, 'publish'
          logging.debug ('HandlerInterface: publishEvent')
          self.ms.publish (name, payload, delay)
          self.ms.commit()
          return


  
