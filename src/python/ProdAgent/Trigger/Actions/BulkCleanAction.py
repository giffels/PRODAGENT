
import logging
import os

from ProdAgent.Trigger.Actions.ActionInterface import ActionInterface
from ProdCommon.Core.GlobalRegistry import registerHandler

class BulkCleanAction(ActionInterface):

   """
   _BulkCleanAction_

   A thin class which is associated to a trigger.
   If the trigger is "triggered" it will emit a job clean action.

   """

   def __init__(self):
      ActionInterface.__init__(self)
      self.args={}

   def invoke(self,id,payload):
      logging.debug("Assembling bulk spec file")
      reduced_cache = os.path.dirname(os.path.dirname(payload))
      NNNN, tarName = id.split('::')
      bulkspec = reduced_cache+'/'+NNNN+'/BulkSpecs'+tarName
      logging.debug('Removing bulkspec: '+bulkspec)
      try:
           os.remove(bulkspec)
      except Exception,ex:
           logging.debug("ERROR removing file: "+str(ex))
      self.trigger.cleanout(id)

registerHandler(BulkCleanAction(),"bulkCleanAction","ProdAgent.Triggers")
