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
        "se-name-1" : "PhEDExNode-1",
        "se-name-2" : "PhEDExNode-2",
        "se-name-3" : "PhEDExNode-3",
        "se-name-4" : "PhEDExNode-4",
        "se-name-5" : "PhEDExNode-5",
        'cmssrm.fnal.gov' : 'T1_FNAL',
        
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
    
