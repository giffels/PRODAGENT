

import logging
import ProdAgentCore.LoggingUtils as LoggingUtils
import MySQLdb


class ConfigDB:
	def __init__(self,run_number):
		self.run_number=run_number


	def getOutputStreamNameByTag(self,tag):
		"""
		Dummy placeholder - should return the repacker output DS name based on tag (and possibly run)
		"""
		ds_name="repacker_out_ds_"+`run_number`+'_'+tag
		return ds_name


	def getSelectionSettings(self,tag):
		"""
		(Possibly) usefull string(s) for repacker's config file
		"""
		return ""

	def getAllTags(self):
		ret=("TestTrig001","TestTrig002","TestTrig003")
		return ret

