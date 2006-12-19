#!/usr/bin/env python
"""
_PhEDExNodeMap_

Real quick and nasty mapping of se-name to phedex node name.

"""



class PhEDExNodeMap:
    """
    _PhEDExNodeMap_

    Static mapping of se name to PhEDEx node name

    """
    _Map = {
       
        'cmssrm.fnal.gov' : 'T1_FNAL_MSS',
        "cmssrm.hep.wisc.edu" : "T2_Wisconsin_Buffer",
        "thpc-1.unl.edu" : "T2_Nebraska_Buffer",
        "cithep59.ultralight.org" : "T2_Caltech_Buffer",
        "dcache.rcac.purdue.edu" : "T2_Purdue_Buffer",
        "t2data2.t2.ucsd.edu" : "T2_UCSD_Buffer",
        "se01.cmsaf.mit.edu" : "T2_MIT_Buffer",
        "ufdcache.phys.ufl.edu" : "T2_Florida_Buffer",
        "castorsrm.pic.es" : "T1_PIC_MSS",
        "castorsrm.ciemat.es" : "T2_Spain_Buffer",
        "srm.cern.ch" : "T1_CERN_MSS",
        "ccsrm.in2p3.fr" : "T1_IN2P3_MSS",
        "ccsrm.in2p3.fr" : "T1_IN2P3_Buffer",
        "maite.iihe.ac.be" : "T2_Belgium_IIHE",
        "dpm01.ifca.es" : "T2_Spain_IFCA",
        'sc.cr.cnaf.infn.it' : "T1_CNAF_MSS",
        'pccms2.cmsfarm1.ba.infn.it' : "T2_Bari_Buffer",
        't2-srm-01.lnl.infn.it' : "T2_Legnaro_Buffer",
        'cmsdpm.pi.infn.it' : "T2_Pisa_Buffer",
        'cmsrm-se01.roma1.infn.it' : "T2_Rome_Buffer",
        'gfe02.hep.ph.ic.ac.uk' : "T2_London_IC_HEP",
        'ralsrma.rl.ac.uk' : "T1_RAL_Buffer",
        'grid100.kfki.hu' : "T2_Budapest_Buffer",
        'heplnx204.pp.rl.ac.uk' : "T2_RutherfordPPD",
        'dgc-grid-34.brunel.ac.uk' : "T2_London_Brunel",
        "grid-srm.physik.rwth-aachen.de" : "T2_RWTH_Buffer",
        "io.hep.kbfi.ee" : "T2_Estonia_Buffer",
        "gridka-dCache.fzk.de" : "T1_FZK_Buffer", 
        "lxfs07.jinr.ru" : "T2_JINR_Buffer",
        "cluster142.knu.ac.kr" : "T2_KNU_Buffer",
        "castorsc.grid.sinica.edu.tw" : "T1_ASGC_Buffer",
        "srm-dcache.desy.de" : "T2_DESY_Buffer",
        "se01-lcg.projects.cscs.ch" : "T2_CSCS_Buffer",
        "lcg60.sinp.msu.ru" : "T2_SINP_Buffer"

        }

    def __init__(self):
        pass


    def translateSE(self, seName):
        """
        _translateSE_

        Convert SE Name to PhEDEx Node name.
        TODO: Exception if missing

        """
        return  self._Map.get(seName, None)
    
