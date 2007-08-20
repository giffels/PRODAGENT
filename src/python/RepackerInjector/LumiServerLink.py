"""
_LumiServerLink_

Provides LumiData from LumiServer and populates JobSpec's config file with lumi data.

"""

__version__ = "$Revision: 1.7 $"
__revision__ = "$Id: LumiServerLink.py,v 1.7 2007/08/06 14:14:01 hufnagel Exp $"
__author__ = "kss"


import logging
import ProdAgentCore.LoggingUtils as LoggingUtils
from lumiException import *
from lumiOptions import LumiOptionParser
from lumiApi import LumiApi
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError, formatEx
import pickle



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



    def setLumiData(self,job_spec_file,job_spec,lumiList):

        lumi_info=self.getLumiInfo(lumiList)
        #print "LUMIINFO",lumi_info
        self._int_setLumiData(job_spec_file,job_spec,lumi_info)



    def getLumiInfo(self,lumiList):

        #
        # FIXME: need to loop over all lumi sections
        #
        runNumber = long(lumiList[0]['RunNumber'])
        lumiSectionNumber = long(lumiList[0]['LumiSectionNumber'])

        lumi_info={"lsnumber":lumiSectionNumber}
        
        if(not self.con):
            return lumi_info

        logging.info("Getting LumiInfo for run %s lumisection %s" % (runNumber,lumiSectionNumber))
        
        lumi_sum=self._getSummary(runNumber,lumiSectionNumber)
        #print "LUMI_SUM",lumi_sum
        lumi_info['avginslumi']=float(lumi_sum.get('instant_et_lumi','0.0'))
        lumi_info['avginslumierr']=float(lumi_sum.get('instant_et_lumi_err','0.0'))
        lumi_info['lumisecqual']=long(float(lumi_sum.get('instant_et_lumi_qlty','0')))
        lumi_info['deadfrac']=float(lumi_sum.get('live_frac','0.0'))

        lumi_info['det_et_sum']=[]
        lumi_info['det_et_err']=[]
        lumi_info['det_et_qua']=[]
        lumi_info['det_occ_sum']=[]
        lumi_info['det_occ_err']=[]
        lumi_info['det_occ_qua']=[]

        lumi_et_det=self._getDetails(runNumber,lumiSectionNumber,"ET")
        old_bunch=0
        for b in lumi_et_det:
            bunch=int(b['bunch_number'])
            if(bunch-old_bunch!=1):
                msg="Error in ET bunch sequence: %d %d"%(old_bunch,bunch)
                raise DBSReaderError(msg)
            old_bunch=bunch
            sum=float(b['ET']['et_lumi'])
            err=float(b['ET']['et_lumi_err'])
            qua=int(float(b['ET']['et_lumi_qlty']))
            lumi_info['det_et_sum'].append(sum)
            lumi_info['det_et_err'].append(err)
            lumi_info['det_et_qua'].append(qua)
            
        lumi_occ_det=self._getDetails(runNumber,lumiSectionNumber,"OCC")
        old_bunch=0
        for b in lumi_occ_det:
            bunch=int(b['bunch_number'])
            if(bunch-old_bunch!=1):
                msg="Error in OCC bunch sequence: %d %d"%(old_bunch,bunch)
                raise DBSReaderError(msg)
            old_bunch=bunch
            sum=float(b['OCC']['occ_lumi'])
            err=float(b['OCC']['occ_lumi_err'])
            qua=int(float(b['OCC']['occ_lumi_qlty']))
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



    def _int_setLumiData(self,job_spec_file,job_spec,lumi_data):
        if(len(lumi_data)<=1):
            logging.info("Insufficient lumi data - ignoring")
            return
        # Set lumi data here
        #print "Set LumiData",lumi_data
        cfgInstance = pickle.loads(job_spec.payload.cfgInterface.rawCfg)
        #print "PRODUCERS:",cfgInstance.producers_()
        # Get producers list (lumi module is EDProducer)
        producers_list=cfgInstance.producers_()
        mod_lumi=producers_list['lumiProducer']
        #print "LumiModule",mod_lumi.parameterNames_(),dir(mod_lumi)
        #Get template pset for the lumi module
        pset_name=mod_lumi.parameterNames_()[0]
        pset=getattr(mod_lumi,pset_name)

        #Clean the template pset name
        delattr(mod_lumi,pset_name)
        #print "LumiModule2",mod_lumi.parameterNames_()

        #Create the real PSet name"
        pset_name="LS"+str(lumi_data['lsnumber'])
        pset.setLabel(pset_name)
        #Set parameters
        pset.avginslumi=lumi_data['avginslumi']
        pset.avginslumierr=lumi_data['avginslumierr']
        pset.lumisecqual=int(lumi_data['lumisecqual'])
        pset.deadfrac=lumi_data['deadfrac']
        pset.lsnumber=int(lumi_data['lsnumber'])

        pset.lumietsum=lumi_data['det_et_sum']
        pset.lumietsumerr=lumi_data['det_et_err']
        pset.lumietsumqual=lumi_data['det_et_qua']
        pset.lumiocc=lumi_data['det_occ_sum']
        pset.lumioccerr=lumi_data['det_occ_err']
        pset.lumioccqual=lumi_data['det_occ_qua']
        
        #Insert the pset into the lumi module
        setattr(mod_lumi,pset_name,pset)
        
        # bla-bla
        #print "DUMP:",cfgInstance.dumpConfig()

        # save spec after update
        job_spec.save(job_spec_file)
        return



lumi_server_dict={}

def getLumiServerLink(args):
    url=args['LumiServerUrl']
    if(lumi_server_dict.has_key(url)):
        return lumi_server_dict[url]
    lsl=LumiServerLink(url=args["LumiServerUrl"],level=args["DbsLevel"])
    lumi_server_dict[url]=lsl
    return lsl
