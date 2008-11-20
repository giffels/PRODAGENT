#!/usr/bin/env python
"""
_OfflineConfiguration_

Configuration for mapping streams and datasets to the various controls and
settings used to process each stream.
"""

__revision__ = "$Id: OfflineConfiguration.py,v 1.2 2008/09/22 15:18:58 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from T0.RecoConfig.ControlFile import Tier0Configuration
from T0.RecoConfig.ControlFile import addDataset

# Default settings for all configurations is that reco is
# disabled.
addDataset(Tier0Configuration, "Default", "Default", do_reco = False,
           scenario = "collisions", OverrideCMSSW = "BOGUS")

# Default framework version
defaultCMSSWVersion = "CMSSW_2_1_12"

# Config files for each scenario.  Filenames must be URLs.  Currently
# only the "file" protocol is supported.
recoConfig = {}
#recoConfig["cosmics"] = "file:///data/cmsprod/CMSSW/CMSSW_2_1_10/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_1_26_cfg.py"
#recoConfig["cosmics"] = "file:///data/cmsprod/CMSSW/CMSSW_2_1_10/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_38T_1_3_cfg.py"
#recoConfig["cosmics"] = "file:///data/cmsprod/CMSSW/CMSSW_2_1_11/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_38T_1_5_cfg.py"
#recoConfig["cosmics"] = "file:///data/cmsprod/CMSSW/CMSSW_2_1_11/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_38T_1_5_cfg_2_1_11.py"
#recoConfig["cosmics"] = "file:///data/cmsprod/CMSSW/CMSSW_2_1_11/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_38T_1_6_cfg.py"
#recoConfig["cosmics"] = "file:///data/cmsprod/CMSSW/CMSSW_2_1_11/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_1_29_cfg.py"
recoConfig["cosmics"] = "file:///data/cmsprod/CMSSW/CMSSW_2_1_12/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_40T_1_1_LazyDownload_cfg.py"
#recoConfig["cosmics"] = "file:///data/cmsprod/CMSSW/CMSSW_2_1_12/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_38T_1_6_LazyDownload_cfg.py"
#recoConfig["cosmics"] = "file:///data/cmsprod/CMSSW/CMSSW_2_1_12/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_1_29_LazyDownload_cfg.py"

def addStreamADataset(datasetName, scenarioName, cmsswVersion = None):
    """
    _addStreamADataset_

    Convience function for setting the configuration for datasets in the "A"
    stream.  If a framework version is not passed in the default will be used.
    """
    if cmsswVersion == None:
        cmsswVersion = defaultCMSSWVersion
        
    addDataset(Tier0Configuration, "A", datasetName, do_reco = True,
               reco_configuration = recoConfig[scenarioName],
               scenario = scenarioName,
               reco_version = cmsswVersion
               )

# Install configurations for the stream "A" datasets.
addStreamADataset("MinimumBias", "cosmics")
addStreamADataset("Cosmics", "cosmics")
addStreamADataset("Calo", "cosmics")

if __name__ == '__main__':
    print Tier0Configuration
