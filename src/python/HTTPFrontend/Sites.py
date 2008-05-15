#!/usr/bin/env python
"""
_SiteMapping_

CherryPy handler for displaying the plot of workflow history

"""

def SiteMap():
    
    T2 = {}
# pisa        
###        T2['T2_BE_IIHE'] = ('iihe',)
###        T2['T2_BE_UCL'] = ('ucl',)
###        T2['T2_DE_DESY'] = ('gridka',)
###        T2['T2_DE_RWTH'] = ('rwth',)
###        T2['T2_IT_Pisa'] = ('pi.infn.it',)

# crabas2
    T2['T2_US_Nebraska'] = ('unl.edu',)
    T2['T2_CH_CSCS'] = ('cscs.ch',)
    T2['T2_TW_Taiwan'] = ('sinica.edu.tw',)
    T2['T2_CN_Beijing'] = ('ac.cn',)
    T2['T2_ES_CIEMAT'] = ('ciemat',)
    T2['T2_ES_IFCA'] = ('ifca.es',)
    T2['T2_FR_CCIN2P3'] = ('in2p3',)
    T2['T2_HU_Budapest'] = ('.hu',)
    T2['T2_IT_Bari'] = ('ba.infn.it',)
    T2['T2_IT_Legnaro'] = ('lnl',)
    T2['T2_IT_Rome'] = ('roma',)
    T2['T2_US_Caltech'] = ('ultralight.org',)
    T2['T2_US_UCSD'] = ('ucsd.edu',)
    T2['T2_US_Purdue'] = ('purdue.edu',)
    T2['T2_US_Florida'] = ('ufl.edu',)
    T2['T2_IT_Pisa'] = ('pi.infn.it',)
    T2['T2_KR_KNU'] = ('knu.ac.kr',)
    T2['T2_UK_SGrid_Bristol'] = ('bris.ac.uk',)
    T2['T2_UK_SGrid_RALPP'] = ('rl.ac.uk',)
#        T2['T2_UK_SouthGrid'] = ('rl.ac.uk',)
    T2['T2_US_MIT'] = ('mit.edu',)
#AF new
    T2['T2_PL_Warsaw'] = ('polgrid.pl',)
# nothing
#        T2['T2_FR_GRIF'] = ()
#        T2['T2_UK_London'] = ('ic','.uk',)#
#        T2['T2_US_All'] = ('fnal.gov',)
#        T2['T2_US_MIT'] = ()
#        T2['T2_US_Wisconsin'] = ()
#        T2['T2_BR_UERJ'] = ('what?!?',)
#        T2['T2_BR_SPRACE'] = ('what?!?',)
    return T2


def SiteRegExp(site):
    if SiteMap().has_key(site):
        return SiteMap()[site]
    else:
        return 'all'
   
