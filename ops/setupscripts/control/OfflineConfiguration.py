#!/usr/bin/env python
"""
_OfflineConfiguration_

Processing configuration for the Tier0.
"""

__revision__ = "$Id: OfflineConfiguration.py,v 1.13 2009/07/02 19:43:56 dmason Exp $"
__version__ = "$Revision: 1.13 $"

from T0.RunConfigCache.Tier0Config import addDataset
from T0.RunConfigCache.Tier0Config import addTier1Skim
from T0.RunConfigCache.Tier0Config import addExpressConfig
from T0.RunConfigCache.Tier0Config import createTier0Config
from T0.RunConfigCache.Tier0Config import setAcquisitionEra
from T0.RunConfigCache.Tier0Config import setConfigVersion
#from T0.RunConfigCache.Tier0Config import setProcessingStyle
from T0.RunConfigCache.Tier0Config import setRepackVersionMapping
from T0.RunConfigCache.Tier0Config import setExpressVersionMapping

# Create the Tier0 configuration object
tier0Config = createTier0Config()

# Set global parameters like the acquisition era and the version of
# the configuration.
#setAcquisitionEra(tier0Config, "HAPPYHAPPYWARMFUZZY_T0TEST_WITHBUNNIESDANCINGAROUND")
setAcquisitionEra(tier0Config, "Commissioning09")
#setAcquisitionEra(tier0Config, "CRUZET09")
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


defaultRecoVersion = "CMSSW_3_1_1"
defaultAlcaVersion = "CMSSW_3_1_1"
defaultDQMVersion = "CMSSW_3_1_1"


defaultProcVersion = "v3"
#repackProcVersion = defaultProcVersion
expressProcVersion = "v4"
recoProcVersion = "v5"
#recoProcVersion = defaultProcVersion
#defaultGlobalTag = "CRAFT_V18P::All"

#31X
defaultGlobalTag = "GR09_31X_V3P::All"

# Create a dictionary that associates a reco configuration with a scenario.
# The configuration must be specified as a url.
recoConfig = {}
alcaConfig = {}
# for CRUZET and BeamSplash reprocessing
#recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_cfg.py?revision=1.44"

#for 3_1_X cosmics at 0T
recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/promptReco_RAW2DIGI_RECO_DQM.py?revision=1.4"

#hacked config to stop crashes
#recoConfig["cosmics"] = "/data/cmsprod/CMSSW/CMSSW_2_2_11/src/Configuration/GlobalRuns/python/recoT0DQM_EvContent_cfg_1.44_hacked.py"
# for EW35 and CRAFT reprocessing
#recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_38T_cfg.py?revision=1.22"
#recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_DBField_cfg.py?revision=1.2"
# Create the default configuration.  Repacking is enabled and everything else
# is turned off.  The default processing style is also set to "Bulk".
#alcaConfig["cosmics"] = "/data/cmsprod/CMSSW/CMSSW_2_2_13/src/step3_V16_ALCA_CRAFT.py"

#31X cosmics
#alcaConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/AlCaRecoCosmics_cfg.py?revision=1.1"
alcaConfig["cosmics"] ="http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_prompt_cfg.py?revision=1.2"

#setProcessingStyle(tier0Config, "Default", "Bulk")
addDataset(tier0Config, "Default",
           default_proc_ver = defaultProcVersion, hltdebug = False,
           reco_proc_ver = recoProcVersion,
           do_reco = False, do_alca = False, do_dqm = False)

# Configure the processing style for the various streams.

#setProcessingStyle(tier0Config, "A", "Bulk")
#setProcessingStyle(tier0Config, "ALCAP0", "Bulk")
#setProcessingStyle(tier0Config, "ALCAPHISYM", "Bulk")
#setProcessingStyle(tier0Config, "ALCAPHISYMHCAL", "Bulk")
#setProcessingStyle(tier0Config, "Calibration", "Bulk")
#setProcessingStyle(tier0Config, "EcalCalibration", "Bulk")
#setProcessingStyle(tier0Config, "DQM", "Bulk")
#setProcessingStyle(tier0Config, "HLTDEBUG", "Bulk")
#setProcessingStyle(tier0Config, "RPCMON", "Bulk")
#setProcessingStyle(tier0Config, "HLTMON", "Bulk")

#setProcessingStyle(tier0Config, "Express", "Express")
#setProcessingStyle(tier0Config, "HLTMON", "Express")

# Actual configuration for datasets.  The Calo, Cosmics and MinimumBias
# datasets will be reconstructed.
addDataset(tier0Config, "Calo",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           custodial_node = "T1_FR_CCIN2P3_MSS",
#           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "Cosmics",
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
addDataset(tier0Config, "MinimumBias",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           custodial_node = "T1_ES_PIC_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,  "Monitor",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           reco_proc_ver = recoProcVersion,
           do_dqm = False,dqm_version=defaultDQMVersion,
           do_alca = False,
           alca_configuration=alcaConfig["cosmics"],
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"HcalHPDNoise",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"RandomTriggers",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"AlCaP0",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"AlCaPhiSymEcal",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"AlCaPhiSymHcal",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"TestEnables",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"EcalLaser",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
#addDataset(tier0Config, "OfflineMonitor",
#           default_proc_ver = defaultProcVersion, scenario = "cosmics",
#           do_reco = True, global_tag = defaultGlobalTag,
#           reco_proc_ver = recoProcVersion,
#           reco_configuration = recoConfig["cosmics"],
#           reco_version = defaultRecoVersion,
#           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"RPCMonitor",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = defaultGlobalTag,
           archival_node = "T0_CH_CERN_MSS")



# set up T1 skimming

# Create a dictionary that associates skim names to config urls.
skimConfig = {}
skimConfig["T1SkimTester"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"
skimConfig["SuperPointing"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/SuperPointing_cfg.py?revision=1.13"
skimConfig["TrackerPointing"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/TrackerPointing_cfg.py?revision=1.10"
skimConfig["HcalHPDFilter"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/HCALHighEnergy_cfg.py?revision=1.4"

addTier1Skim(tier0Config, "Skim1", "RECO", "Cosmics", "CMSSW_3_1_1", "v1",
             skimConfig["T1SkimTester"], True)
addTier1Skim(tier0Config, "Skim2", "RECO", "Calo", "CMSSW_3_1_1", "v2",
             skimConfig["T1SkimTester"], True)
addTier1Skim(tier0Config, "Skim3", "RECO", "MinimumBias", "CMSSW_3_1_1", "v3",
             skimConfig["T1SkimTester"], True)





# set up Express handling

# actual express configuration
# Create a dictionary that associates express processing config urls to names.
expressProcConfig = {}
#expressProcConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_2_2_6/src/recoT0DQM_EvContent_Express_cfg_V16_1.44.py"
#expressProcConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_2_2_13/src/raw2digi_reco_alcaCombined_express_cfg.py"
#expressProcConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_2_2_13/src/raw2digi_reco_alcaCombined_express_cfg_V18P.py"


#For 31x
#expressProcConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_3_1_0/src/recoAlcaProc.py"
expressProcConfig["default"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/expressReco_RAW2DIGI_RECO_DQM_ALCA.py?revision=1.3" 


# Create a dictionary that associated express merge packing config urls to names

expressMergePackConfig = {}
#expressMergePackConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_2_2_6/src/mergepacktestwithPrescales.py"
#expressMergePackConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_2_2_13/src/alCaRecoSplitting_express_cfg.py"

#For 31x
#expressMergePackConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_3_1_0/src/recoAlcaMergePack.py"
expressMergePackConfig["default"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_express_cfg.py?revision=1.3"


expressProcConfig["alca"] = "/data/cmsprod/CMSSW/CMSSW_2_2_13/src/step23_EcalCalPi0Calib_ALCA_CRAFT_processing.py"
expressMergePackConfig["alca"] = "/data/cmsprod/CMSSW/CMSSW_2_2_13/src/step23_EcalCalPi0Calib_ALCA_CRAFT_alcamerge.py"




#addExpressConfig(tier0Config, "Express",
#                  expressProcConfig["default"],
#                  expressMergePackConfig["default"], False,defaultProcVersion)

addExpressConfig(tier0Config, "Express",
                 proc_config = expressProcConfig["default"],
                 data_tiers = [ "RAW", "RECO", "ALCARECO" ],
                 alcamerge_config = expressMergePackConfig["default"],
                 splitInProcessing = False,
                 proc_ver = expressProcVersion)

addExpressConfig(tier0Config, "HLTMON",
                 proc_config = expressProcConfig["default"],
                 data_tiers = [ "FEVTHLTALL" ],
                 #data_tiers = [ "FEVT" ],
                 splitInProcessing = True,
                 proc_ver = expressProcVersion)

# uncomment this and comment out the bulk stuff above for this stream to do
# this as express

#addExpressConfig(tier0Config, "ALCAP0",
#                 proc_config = expressProcConfig["alca"],
#                 data_tiers = [ "ALCARECO" ],
#                 alcamerge_config = expressMergePackConfig["alca"],
#                 splitInProcessing = True,
#                 proc_ver = expressProcVersion)


#Mappings for 311 online running
setExpressVersionMapping(tier0Config, "CMSSW_3_1_0", "CMSSW_3_1_1")

setExpressVersionMapping(tier0Config, "CMSSW_2_2_13", "CMSSW_6_6_6")
setExpressVersionMapping(tier0Config, "CMSSW_2_2_12", "CMSSW_6_6_6")
setExpressVersionMapping(tier0Config, "CMSSW_2_2_11", "CMSSW_6_6_6")
setExpressVersionMapping(tier0Config, "CMSSW_2_2_10", "CMSSW_6_6_6")


#Set express processing version remapping
# Policy is to express process with repack CMSSW version.
#setExpressVersionMapping(tier0Config, "CMSSW_2_2_10", "CMSSW_2_2_13_offpatch1")
#setExpressVersionMapping(tier0Config, "CMSSW_2_2_11", "CMSSW_2_2_13_offpatch1")
#setExpressVersionMapping(tier0Config, "CMSSW_2_2_12", "CMSSW_2_2_13_offpatch1")
#setExpressVersionMapping(tier0Config, "CMSSW_2_2_13", "CMSSW_2_2_13_offpatch1")

#Set mappings to send 31x tests to trash
#setExpressVersionMapping(tier0Config, "CMSSW_3_1_0", "CMSSW_6_6_6")
#setExpressVersionMapping(tier0Config, "CMSSW_3_1_0_pre10", "CMSSW_6_6_6")
#setExpressVersionMapping(tier0Config, "CMSSW_3_1_0_pre11", "CMSSW_6_6_6")
#setRepackVersionMapping(tier0Config, "CMSSW_3_1_0", "CMSSW_6_6_6")
#setRepackVersionMapping(tier0Config, "CMSSW_3_1_0_pre10", "CMSSW_6_6_6")
#setRepackVersionMapping(tier0Config, "CMSSW_3_1_0_pre11", "CMSSW_6_6_6")



#Mappings to fail out anything but 31x
setRepackVersionMapping(tier0Config, "CMSSW_2_2_10", "CMSSW_6_6_6")
setRepackVersionMapping(tier0Config, "CMSSW_2_2_11", "CMSSW_6_6_6")
setRepackVersionMapping(tier0Config, "CMSSW_2_2_12", "CMSSW_6_6_6")
setRepackVersionMapping(tier0Config, "CMSSW_2_2_13", "CMSSW_6_6_6")



# Setup the mappings between the framework version used to take a run and the
# version that should be used to repack it.
#setRepackVersionMapping(tier0Config, "CMSSW_2_0_10", "CMSSW_2_0_12")
#setRepackVersionMapping(tier0Config, "CMSSW_2_0_4", "CMSSW_2_0_12")
#setRepackVersionMapping(tier0Config, "CMSSW_2_0_8", "CMSSW_2_0_12")
#setRepackVersionMapping(tier0Config, "CMSSW_2_1_X_2008-08-16-0300", "CMSSW_2_1_8")
#setRepackVersionMapping(tier0Config, "CMSSW_2_1_0", "CMSSW_2_1_8")
#setRepackVersionMapping(tier0Config, "CMSSW_2_1_1", "CMSSW_2_1_8")
#setRepackVersionMapping(tier0Config, "CMSSW_2_1_4", "CMSSW_2_1_8")
#setRepackVersionMapping(tier0Config, "CMSSW_2_1_9", "CMSSW_2_1_9")
if __name__ == '__main__':
    print tier0Config
