
import logging
import ProdAgentCore.LoggingUtils as LoggingUtils
import MySQLdb
#from DBSAPI.dbsApi import DbsApi
#from DBSAPI.dbsException import *
#from DBSAPI.dbsApiException import *


class DbsLink:
	def __init__(self,**kw):
		self.con=None
		try:
			self.con=MySQLdb.connect(db=kw["db"],host=kw["host"],user=kw["user"],passwd=kw["passwd"])
		except Exception,ex:
			print "ERROR:",ex
			if(self.con):
				self.con.close()
				self.con=None
			return


	def close(self):
		try:
			if(self.con):
				self.con.close()
				self.con=None
		except Exception,ex:
			print "Can not close connection, error",ex


	def __del__(self):
		print "Closing connection"
		self.close()


	def commit(self):
		self.con.commit()


		
	def setFileStatus(self,lfn,status_word):
		cur=None
		try:
			cur=self.con.cursor()
			cur.execute("update Files set QueryableMetadata=%s where LogicalFileName=%s",(status_word,lfn))
		except Exception,ex:
			print "Can not set status, error",ex
			return -1
		return 0



	def getFileTriggerTags(self,file_id):
		cur=None
		sql="""select TriggerTag from FileTriggerTag where Fileid=%s and NumberOfEvents > 0"""
		ret=[]
		try:
			cur=self.con.cursor()
			cur.execute(sql,(file_id,))
			while(1):
				row=cur.fetchone()
				if(not row):
					break				
				ret.append(row[0])
		except Exception,ex:
			print "Can not get tags, error",ex
			return -1,[]
		return 0,ret



	def poll_for_files(self,pri_ds,pro_ds):
		sql="""select b.ID, f.ID, f.LogicalFileName from Block b, Files f, ProcessedDataset pro, PrimaryDataset pri
                       where b.Dataset=pro.ID and pro.PrimaryDataset=pri.ID and pri.Name=%s and pro.Name=%s
                         and f.Block=b.ID and b.OpenForWriting=0 and f.QueryableMetadata is NULL
                       order by 1,2"""
		cur=None
		res=[]
		try:
			cur=self.con.cursor()
			cur.execute(sql,(pri_ds,pro_ds))
			while(1):
				row=cur.fetchone()
				if(not row):
					break
				print row
				file_id=row[1]
				res_tag,tags=self.getFileTriggerTags(file_id)
				if(res_tag<0):
					raise "Error: can not get tags"
				lfn=row[2]
				res.append((lfn,tags))
			cur.close()
		except Exception,ex:
			print "ERROR:",ex
			return []
		print res
		return res
		
		
