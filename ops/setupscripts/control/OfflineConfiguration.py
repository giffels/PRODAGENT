#!/usr/bin/env python
"""
_OfflineConfiguration_

Processing configuration for the Tier0.
"""

__revision__ = "$Id: OfflineConfiguration.py,v 1.2 2008/12/11 20:41:04 dmason Exp $"
__version__ = "$Revision: 1.2 $"

from T0.RunConfigCache.Tier0Config import addDataset
from T0.RunConfigCache.Tier0Config import createTier0Config
from T0.RunConfigCache.Tier0Config import setAcquisitionEra
from T0.RunConfigCache.Tier0Config import setConfigVersion
from T0.RunConfigCache.Tier0Config import setProcessingStyle
from T0.RunConfigCache.Tier0Config import setRepackVersionMapping

# Create the Tier0 configuration object
tier0Config = createTier0Config()

# Set global parameters like the acquisition era and the version of
# the configuration.
setAcquisitionEra(tier0Config, "HAPPYHAPPYWARMFUZZY_T0TEST_WITHBUNNIESDANCINGAROUND")
#setAcquisitionEra(tier0Config, "Commissioning08")
#setAcquisitionEra(tier0Config, "AllRunsTest")
setConfigVersion(tier0Config, __version__)

# Setup some useful defaults: processing version, reco framework version,
# global tag.
defaultProcVersion = "PFTHPFTHPTHFPFTHPHTH-v2"


#######################################################################
# Era info used in preCraft re-repack

# CruzetAll eras
#defaultGlobalTag = "CRUZETALL_V8::All"
#
#defaultProcVersion = "CruzetAll_noHLT-TEMPORARYFORVALIDATIONONLY-v3"
#defaultProcVersion = "CruzetAll_FU_L1Basic-TEMPORARYFORVALIDATIONONLY-v3"
#defaultProcVersion = "CruzetAll_FU_L1More-TEMPORARYFORVALIDATIONONLY-v3"
#defaultProcVersion = "CruzetAll_HLT_L1Basic-TEMPORARYFORVALIDATIONONLY-v3"
#defaultProcVersion = "CruzetAll_HLT_L1More-TEMPORARYFORVALIDATIONONLY-v3"


# BeamSplash eras
#defaultGlobalTag  -- now same as CruzetAll
#
#defaultProcVersion = "BeamSplash_noHLT-TEMPORARYFORVALIDATIONONLY-v3"
#defaultProcVersion = "BeamSplash_FU_L1Basic-TEMPORARYFORVALIDATIONONLY-v3"
#defaultProcVersion = "BeamSplash_FU_L1More-TEMPORARYFORVALIDATIONONLY-v3"


# EW35 era
defaultProcVersion = "EW35-TEMPORARYFORVALIDATIONONLY-v3"
#
defaultGlobalTag = "EW35_V4::All"


#
######################################################################


defaultRecoVersion = "CMSSW_2_2_5"



#defaultGlobalTag = "CRAFT_ALL_V7::All"

# Create a dictionary that associates a reco configuration with a scenario.
# The configuration must be specified as a url.
recoConfig = {}

# for CRUZET and BeamSplash reprocessing
#recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_cfg.py?revision=1.37"
# for EW35 and CRAFT reprocessing
#recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_38T_cfg.py?revision=1.22"
recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_DBField_cfg.py?revision=1.2"
# Create the default configuration.  Repacking is enabled and everything else
# is turned off.  The default processing style is also set to "Bulk".
setProcessingStyle(tier0Config, "Default", "Bulk")
addDataset(tier0Config, "Default", "Default",
           default_proc_ver = defaultProcVersion, hltdebug = False,
           do_reco = False, do_alca = False, do_dqm = False)

# Configure the processing style for the various streams.
setProcessingStyle(tier0Config, "A", "Bulk")
setProcessingStyle(tier0Config, "HLTDEBUG", "Bulk")

# Actual configuration for datasets.  The Calo, Cosmics and MinimumBias
# datasets will be reconstructed.
addDataset(tier0Config, "A", "Calo",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion)
addDataset(tier0Config, "A", "Cosmics",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion)
addDataset(tier0Config, "A", "MinimumBias",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion)
addDataset(tier0Config, "A", "BeamHalo",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion)
addDataset(tier0Config, "A", "BarrelMuon",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion)
addDataset(tier0Config, "A", "EndcapsMuon",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion)

# Setup the mappings between the framework version used to take a run and the
# version that should be used to repack it.
setRepackVersionMapping(tier0Config, "CMSSW_2_0_10", "CMSSW_2_0_12")
setRepackVersionMapping(tier0Config, "CMSSW_2_0_4", "CMSSW_2_0_12")
setRepackVersionMapping(tier0Config, "CMSSW_2_0_8", "CMSSW_2_0_12")
setRepackVersionMapping(tier0Config, "CMSSW_2_1_X_2008-08-16-0300", "CMSSW_2_1_8")
setRepackVersionMapping(tier0Config, "CMSSW_2_1_0", "CMSSW_2_1_8")
setRepackVersionMapping(tier0Config, "CMSSW_2_1_1", "CMSSW_2_1_8")
setRepackVersionMapping(tier0Config, "CMSSW_2_1_4", "CMSSW_2_1_8")
#setRepackVersionMapping(tier0Config, "CMSSW_2_1_9", "CMSSW_2_1_9")
if __name__ == '__main__':
    print tier0Config
