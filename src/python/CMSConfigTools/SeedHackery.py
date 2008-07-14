#!/usr/bin/env python
"""
_SeedHackery_

Random Seed Search and replace interim hack until random seed service is
ready

"""
from CMSConfigTools.Search import PSetSearch
from SeedGen.SeedGen import getSeedGen

_SeedGenerator = getSeedGen()


_KnownSeeds = []


#  //
# // Pythia Random Seed
#//
search1 = PSetSearch()
search1['Module'] = "PythiaSource"
search1['PSets'] = ['PythiaParameters']
search1['Parameter'] = 'Pydatr_mrpy'

search2 = PSetSearch()
search2['Module'] = "MadeUpSource"
search2['PSets'] = ['SomeParameters']
search2['Parameter'] = 'SeedyDive'


_KnownSeeds.append(search1)
_KnownSeeds.append(search2)



def randomSeedHack(cfgInstance):
    """
    _randomSeedHack_

    Search cfgInstance for all known seeds defined as PSetSearch
    objects in _KnownSeeds list and replace them with a new seed if they
    exist

    """
    
    for search in _KnownSeeds:
        if search(cfgInstance) != None:
            search.insertValue(cfgInstance, _SeedGenerator.createSeed())
    return

