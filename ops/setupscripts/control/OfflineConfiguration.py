#!/usr/bin/env python
"""
_OfflineConfiguration_

Processing configuration for the Tier0.
"""

__revision__ = "$Id: OfflineConfiguration.py,v 1.7 2009/05/11 16:03:10 dmason Exp $"
__version__ = "$Revision: 1.7 $"

from T0.RunConfigCache.Tier0Config import addDataset
from T0.RunConfigCache.Tier0Config import addTier1Skim
from T0.RunConfigCache.Tier0Config import addExpressConfig
from T0.RunConfigCache.Tier0Config import createTier0Config
from T0.RunConfigCache.Tier0Config import setAcquisitionEra
from T0.RunConfigCache.Tier0Config import setConfigVersion
from T0.RunConfigCache.Tier0Config import setProcessingStyle
from T0.RunConfigCache.Tier0Config import setRepackVersionMapping
from T0.RunConfigCache.Tier0Config import setExpressVersionMapping

# Create the Tier0 configuration object
tier0Config = createTier0Config()

# Set global parameters like the acquisition era and the version of
# the configuration.
#setAcquisitionEra(tier0Config, "HAPPYHAPPYWARMFUZZY_T0TEST_WITHBUNNIESDANCINGAROUND")
setAcquisitionEra(tier0Config, "Commissioning09")
#setAcquisitionEra(tier0Config, "AllRunsTest")
setConfigVersion(tier0Config, __version__)

# Setup some useful defaults: processing version, reco framework version,
# global tag.
#defaultProcVersion = "PFTHPFTHPTHFPFTHPHTH-v4EW35"


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
#defaultProcVersion = "EW35-v1"
#
#defaultGlobalTag = "EW35_V4::All"


#
######################################################################


defaultRecoVersion = "CMSSW_2_2_11"
defaultAlcaVersion = "CMSSW_2_2_11"
defaultDQMVersion = "CMSSW_2_2_11"


defaultProcVersion = "v1"
repackProcVersion = defaultProcVersion
#recoProcVersion = "PFTHPFTHPTHFPFTHPHTH-v12CRAFT-testingtesting"
recoProcVersion = defaultProcVersion
defaultGlobalTag = "CRAFT_V15P::All"

# Create a dictionary that associates a reco configuration with a scenario.
# The configuration must be specified as a url.
recoConfig = {}
alcaConfig = {}
# for CRUZET and BeamSplash reprocessing
#recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_cfg.py?revision=1.44"
#hacked config to stop crashes
recoConfig["cosmics"] = "/data/cmsprod/CMSSW/CMSSW_2_2_11/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_cfg_1.44_hacked.py"
# for EW35 and CRAFT reprocessing
#recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_38T_cfg.py?revision=1.22"
#recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_DBField_cfg.py?revision=1.2"
# Create the default configuration.  Repacking is enabled and everything else
# is turned off.  The default processing style is also set to "Bulk".
alcaConfig["cosmics"] = "/data/cmsprod/CMSSW/CMSSW_2_2_11/src/step3_V15_ALCA_CRAFT.py"
setProcessingStyle(tier0Config, "Default", "Bulk")
addDataset(tier0Config, "Default", "Default",
           default_proc_ver = defaultProcVersion, hltdebug = False,
           reco_proc_ver = recoProcVersion,
           do_reco = False, do_alca = False, do_dqm = False)

# Configure the processing style for the various streams.

setProcessingStyle(tier0Config, "Express", "Express")
setProcessingStyle(tier0Config, "A", "Bulk")
setProcessingStyle(tier0Config, "ALCAP0", "Bulk")
setProcessingStyle(tier0Config, "ALCAPHISYM", "Bulk")
setProcessingStyle(tier0Config, "ALCAPHISYMHCAL", "Bulk")
setProcessingStyle(tier0Config, "Calibration", "Bulk")
setProcessingStyle(tier0Config, "EcalCalibration", "Bulk")
setProcessingStyle(tier0Config, "DQM", "Bulk")
setProcessingStyle(tier0Config, "HLTDEBUG", "Bulk")
setProcessingStyle(tier0Config, "HLTMON", "Bulk")
setProcessingStyle(tier0Config, "RPCMON", "Bulk")



# Actual configuration for datasets.  The Calo, Cosmics and MinimumBias
# datasets will be reconstructed.
addDataset(tier0Config, "A", "Calo",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           custodial_node = "T1_FR_CCIN2P3_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "A", "Cosmics",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = True, alca_version= defaultAlcaVersion, 
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["cosmics"],
           custodial_node = "T1_IT_CNAF_MSS",
           archival_node = "T0_CH_CERN_MSS"
           )
addDataset(tier0Config, "A", "MinimumBias",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           custodial_node = "T1_ES_PIC_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "DQM", "Monitor",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           reco_proc_ver = recoProcVersion,
           do_dqm = False,dqm_version=defaultDQMVersion,
           do_alca = False,
           alca_configuration=alcaConfig["cosmics"],
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "HLTDEBUG", "Monitor",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           reco_proc_ver = recoProcVersion,
           do_dqm = False,dqm_version=defaultDQMVersion,
           do_alca = False,
           alca_configuration=alcaConfig["cosmics"],
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"A","HcalHPDNoise",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"A","RandomTriggers",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"ALCAP0","AlCaP0",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"ALCAPHISYM","AlCaPhiSymEcal",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"ALCAPHISYMHCAL","AlCaPhiSymHcal",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"Calibration","TestEnables",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"EcalCalibration","EcalLaser",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "HLTMON", "OfflineMonitor",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"RPCMON","RPCMonitor",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")



# set up T1 skimming

# Create a dictionary that associates skim names to config urls.
skimConfig = {}
skimConfig["SuperPointing"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/SuperPointing_cfg.py?revision=1.12"
skimConfig["TrackerPointing"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/TrackerPointing_cfg.py?revision=1.9"
skimConfig["HcalHPDFilter"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/HcalHPDFilter_cfg.py?revision=1.2"

addTier1Skim(tier0Config, "Skim1", "A", "RECO", "Cosmics", "CMSSW_2_2_11", "v1",
             skimConfig["SuperPointing"], False)
addTier1Skim(tier0Config, "Skim2", "A", "RECO", "Cosmics", "CMSSW_2_2_11", "v2",
             skimConfig["TrackerPointing"], False)
addTier1Skim(tier0Config, "Skim3", "A", "RECO", "Calo", "CMSSW_2_2_11", "v3",
             skimConfig["HcalHPDFilter"], False)


# set up Express handling

# actual express configuration
# Create a dictionary that associates express processing config urls to names.
expressProcConfig = {}
expressProcConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_2_2_6/src/recoT0DQM_EvContent_Express_cfg_V15_1.44_hacked.py"

# Create a dictionary that associated express merge packing config urls to names

expressMergePackConfig = {}
expressMergePackConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_2_2_6/src/recoT0DQM_EvContent_Express_cfg_V15_1.44_hacked.py"

addExpressConfig(tier0Config, "Express",
                  expressProcConfig["default"],
                  expressMergePackConfig["default"], False)


#Set express processing version remapping
setExpressVersionMapping(tier0Config, "CMSSW_2_2_10", "CMSSW_2_2_11")

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
