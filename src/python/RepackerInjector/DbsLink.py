
import logging

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError, formatEx


class DbsLink:

    def __init__(self,**kw):
        self.con=None
        logging.info("Connecting to DBS [%s]" % kw['url'])
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
            logging.info("Can not set status: %s" % ex)
            return -1
        return 0



    def poll_for_files(self,pri_ds,pro_ds,run):
        datasetpath="/"+pri_ds+"/"+pro_ds+"/RAW"
        res=[]
        try:
            file_list=self.con.listLFNs(path=datasetpath, queryableMetaData="NOTSET")
            #file_list=self.con.listLFNs(path=datasetpath)
            for i in file_list:
                lfn=i['LogicalFileName']
                #check if LFN belongs to correct run
                lumiList = self.con.listFileLumis(lfn)
                file_lumis=[]
                for lumi in lumiList:
                    if lumi['RunNumber'] == run:
                        file_lumis.append(lumi)

                if( len(file_lumis) > 0 ):
                    tags_list=i['FileTriggerMap']
                    tags={}
                    for t in tags_list:
                        n=t['TriggerTag']
                        v=t['NumberOfEvents']
                        if(v>0 or len(n)>0):
                            tags[n]=v
                        else:
                            #print "Ignoring empty tag [%s:%s]"%(n,v)
                            #print lfn,tags
                            res.append((lfn,tags,file_lumis))
        except Exception,ex:
            logging.info("ERROR: %s" % ex)
            return []
        print res
        return res
