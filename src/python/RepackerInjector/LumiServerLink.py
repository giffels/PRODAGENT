

import logging
import ProdAgentCore.LoggingUtils as LoggingUtils
from lumiException import *
from lumiOptions import LumiOptionParser
from lumiApi import LumiApi
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError, formatEx


class LumiServerLink:
    def __init__(self,**kw):
        self.con=None
        self.cache={}
        self.lumiserver_url=kw['url']
        try:
            if(self.lumiserver_url):
                logging.info("Connecting to LumiServer [%s]"%(kw['url'],))
                self.con = LumiApi(kw)
            else:
                self.con=None
                logging.info("IGNORING LUMI DATA - LumiServerUrl was not set !!!")
        except LumiException, ex:
            msg = "Error in LumiServerLink with LumiApi\n"
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)


    def getLumiInfo(self,run_number,lumisection):
        lumi_info={"lsnumber":long(lumisection)}
        
        if(not self.con):
            return lumi_info
            
        logging.info("Getting LumiInfo for run %s lumisection %s"%(str(run_number),str(lumisection)))
        
        lumi_sum=self._getSummary(run_number,lumisection)
        lumi_info['avglumi']=float(lumi_sum.get('delivered_et_lumi','0.0'))
        lumi_info['avglumierr']=float(lumi_sum.get('delivered_et_lumi_err','0.0'))
        lumi_info['lumisecqual']=long(float(lumi_sum.get('delivered_et_lumi_qlty','0')))
        lumi_info['livefrac']=float(lumi_sum.get('live_frac','0.0'))

        lumi_info['det_et_sum']=[]
        lumi_info['det_et_err']=[]
        lumi_info['det_et_qua']=[]
        lumi_info['det_occ_sum']=[]
        lumi_info['det_occ_err']=[]
        lumi_info['det_occ_qua']=[]

        lumi_et_det=self._getDetails(run_number,lumisection,"ET")
        old_bunch=0
        for b in lumi_et_det:
            bunch=int(b['bunch_number'])
            if(bunch-old_bunch!=1):
                msg="Error in ET bunch sequence: %d %d"%(old_bunch,bunch)
                raise DBSReaderError(msg)
            old_bunch=bunch
            sum=float(b['ET']['et_lumi'])
            err=float(b['ET']['et_lumi_err'])
            qua=float(b['ET']['et_lumi_qlty'])
            lumi_info['det_et_sum'].append(sum)
            lumi_info['det_et_err'].append(err)
            lumi_info['det_et_qua'].append(qua)
            
        lumi_occ_det=self._getDetails(run_number,lumisection,"OCC")
        old_bunch=0
        for b in lumi_occ_det:
            bunch=int(b['bunch_number'])
            if(bunch-old_bunch!=1):
                msg="Error in OCC bunch sequence: %d %d"%(old_bunch,bunch)
                raise DBSReaderError(msg)
            old_bunch=bunch
            sum=float(b['OCC']['occ_lumi'])
            err=float(b['OCC']['occ_lumi_err'])
            qua=float(b['OCC']['occ_lumi_qlty'])
            lumi_info['det_occ_sum'].append(sum)
            lumi_info['det_occ_err'].append(err)
            lumi_info['det_occ_qua'].append(qua)

        return lumi_info



    def _getSummary(self,run,lumisection):
        try:
            logging.info("Getting LumiInfo SUMMARY for run %s lumisection %s"%(str(run),str(lumisection)))
            cachekey="_sum_%s_%s"%(str(run),str(lumisection))
            ret=self.cache.get(cachekey,{})
            if(len(ret)>0):
                #print "Got from cache for %s"%cachekey
                return ret
            ret=self.con.listLumiSummary(str(run),str(lumisection),"ET")
            #print "LUMISUMMARY",ret
            ret=ret[0]
            self.cache[cachekey]=ret
            return ret
        except LumiException, ex:
            msg="Caught LUMIServer Exception: %s: %s "  % (ex.msg, ex.code )
            print msg
            #raise DBSReaderError(msg)
            # XXX MUST SET DATA QUALITY FLAG
            return {}


    def _getDetails(self,run,lumisection,lumi_option):
        try:
            logging.info("Getting LumiInfo DETAILS %s for run %s lumisection %s"%(lumi_option,str(run),str(lumisection)))
            cachekey="_%s_det_%s_%s"%(lumi_option,str(run),str(lumisection))
            ret=self.cache.get(cachekey,{})
            if(len(ret)>0):
                #print "Got from cache for %s"%cachekey
                return ret
            ret=self.con.listLumiByBunch(str(run),str(lumisection),lumi_option)
            self.cache[cachekey]=ret
            #print "LUMI_%s_DET"%lumi_option,ret
            return ret
        except LumiException, ex:
            msg="Caught LUMIServer Exception: %s: %s "  % (ex.msg, ex.code )
            print msg
            #raise DBSReaderError(msg)
            # XXX MUST SET DATA QUALITY FLAG
            return {}

