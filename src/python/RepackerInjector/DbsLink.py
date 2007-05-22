
import logging
import ProdAgentCore.LoggingUtils as LoggingUtils
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError, formatEx


class DbsLink:
	def __init__(self,**kw):
		self.con=None
		print "Connecting to DBS [%s]"%(kw['url'],)
		try:
			self.con = DbsApi(kw)
		except DbsException, ex:
			msg = "Error in DBSReader with DbsApi\n"
			msg += "%s\n" % formatEx(ex)
			raise DBSReaderError(msg)


	def close(self):
		pass


	def commit(self):
		pass


		
	def setFileStatus(self,lfn,status_word):
		try:
			self.con.updateFileMetaData(lfn,status_word)
		except Exception,ex:
			print "Can not set status, error",ex
			return -1
		return 0



	def poll_for_files(self,pri_ds,pro_ds):
		dbspath="/"+pri_ds+"/"+pro_ds+"/RAW"
		res=[]
		try:
			file_list=self.con.listLFNs(path=dbspath, queryableMetaData="NOTSET")
			for i in file_list:
				lfn=i['LogicalFileName']
				tags_list=i['FileTriggerMap']
				tags={}
				for t in tags_list:
					n=t['TriggerTag']
					v=t['NumberOfEvents']
					if(v>0 or len(n)>0):
						tags[n]=v
					else:
						print "Ignoring empty tag [%s:%s]"%(n,v)
				print lfn,tags
				res.append((lfn,tags))
		except Exception,ex:
			print "ERROR:",ex
			return []
		print res
		return res
		
		
