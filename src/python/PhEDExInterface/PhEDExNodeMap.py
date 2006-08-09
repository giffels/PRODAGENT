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
        "dcache.rcac.purdue.edu" "T2_Purdue_Buffer",
        "t2data2.t2.ucsd.edu" : "T2_UCSD_Buffer",
        "se01.cmsaf.mit.edu" : "T2_MIT_Buffer",
        "ufdcache.phys.ufl.edu" "T2_Florida_Buffer",
                  
        

        
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
    
