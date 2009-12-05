#!/usr/bin/env python
"""
_OfflineConfiguration_

Processing configuration for the Tier0.
"""

__revision__ = "$Id: OfflineConfiguration.py,v 1.22 2009/11/20 04:19:17 dmason Exp $"
__version__ = "$Revision: 1.22 $"

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
setAcquisitionEra(tier0Config, "BeamCommissioning09")
#setAcquisitionEra(tier0Config, "CRAFT09")
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


defaultRecoVersion = "CMSSW_3_3_5_patch3"
#experimentalRecoVersion = "CMSSW_3_3_5_patch1"
#defaultRecoVersion = "CMSSW_3_2_1"
defaultAlcaVersion = defaultRecoVersion
defaultDQMVersion = defaultRecoVersion


defaultProcVersion = "v1"
#repackProcVersion = defaultProcVersion
expressProcVersion = "v2"
recoProcVersion = "v2"
#recoProcVersion = defaultProcVersion
#defaultGlobalTag = "CRAFT_V18P::All"

#31X
#defaultGlobalTag = "GR09_31X_V6P::All"
defaultGlobalTag = "GR09_E_V7::All"
expressGlobalTag = defaultGlobalTag
promptrecoGlobalTag = "GR09_P_V7::All"
cosmicsrecoGlobalTag = "GR09_P_V7COS::All"


#
# Create a dictionary that associates a reco configuration with a scenario.
# The configuration must be specified as a url.
recoConfig = {}
alcaConfig = {}
# for CRUZET and BeamSplash reprocessing
#recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_cfg.py?revision=1.44"

#for 3_1_X cosmics at 0T
#recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/promptReco_RAW2DIGI_RECO_DQM.py?revision=1.4"

#3_1_1 reco config with combined alca
recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/promptReco_RAW2DIGI_RECO_DQM_ALCA.py?revision=1.12"

#recoConfig["collision"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/promptCollisionReco_RAW2DIGI_L1Reco_RECO_DQM_ALCA.py?revision=1.11"

recoConfig["collision"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/promptCollisionReco_FirstCollisions_RAW2DIGI_L1Reco_RECO_DQM_ALCA.py?revision=1.4"


#recoConfig["collision"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/promptCollisionReco_PixellessTesting_RAW2DIGI_L1Reco_RECO_DQM_ALCA.py?revision=1.1"

recoConfig["hcalnzs"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/promptCollisionNZSReco_RAW2DIGI_L1Reco_RECO_DQM_ALCA.py?revision=1.1"


#promptreco configs for alcaraw datasets
recoConfig["AlCaP0"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alcareco_AlCaP0_cfg.py?revision=1.2"
recoConfig["AlCaPhiSymEcal"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alcareco_AlCaPhiSymEcal_cfg.py?revision=1.2"
recoConfig["AlCaPhiSymHcal"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alcareco_AlCaPhiSymHcal_cfg.py?revision=1.2"

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
alcaConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_Cosmics_cfg.py?revision=1.1"
alcaConfig["calo"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_prompt_Calo_cfg.py?revision=1.2"
alcaConfig["minbias"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_MinimumBias_cfg.py?revision=1.1"

#for alcaraw streams
alcaConfig["AlCaP0"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_AlCaP0_cfg.py?revision=1.3"
alcaConfig["AlCaPhiSymEcal"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_AlCaPhiSymEcal_cfg.py?revision=1.2"
alcaConfig["AlCaPhiSymHcal"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_AlCaPhiSymHcal_cfg.py?revision=1.3"

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

addDataset(tier0Config, "BeamHalo",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["cosmics"],
#           custodial_node = "T1_FR_CCIN2P3_MSS",
           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS"
           )
addDataset(tier0Config, "Calo",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
           custodial_node = "T1_DE_KIT_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "Cosmics",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = cosmicsrecoGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = True, alca_version= defaultAlcaVersion, 
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["cosmics"],
           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS"
           )
addDataset(tier0Config, "HcalNZS",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_configuration = recoConfig["hcalnzs"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = True, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["AlCaPhiSymHcal"],
           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS"
           )
addDataset(tier0Config, "MinimumBias",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = True, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["minbias"],
           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "MinimumBiasNoCalo",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
           custodial_node = "T1_ES_PIC_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "PhysicsMuonBkg",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultRecoVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "ZeroBias",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = True, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
           custodial_node = "T1_IT_CNAF_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "ZeroBiasB",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
           custodial_node = "T1_DE_KIT_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "ZeroBiasBnotT0",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
#           custodial_node = "T1_DE_KIT_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"HcalHPDNoise",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"RandomTriggers",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
           custodial_node = "T1_ES_PIC_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"AlCaP0",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_configuration = recoConfig["AlCaP0"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_alca = True, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["AlCaP0"],
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"AlCaPhiSymEcal",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_configuration = recoConfig["AlCaPhiSymEcal"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_alca = True, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["AlCaPhiSymEcal"],
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"AlCaPhiSymHcal",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
           reco_configuration = recoConfig["AlCaPhiSymHcal"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["AlCaPhiSymHcal"],
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"TestEnables",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"LogMonitor",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"EcalLaser",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
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
           do_reco = False, global_tag = promptrecoGlobalTag,
           custodial_node = "T1_ES_PIC_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"FEDMonitor",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
           custodial_node = "T1_ES_PIC_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"RandomTriggersOpen",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"Test",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
           archival_node = "T0_CH_CERN_MSS")

# set up T1 skimming

# Create a dictionary that associates skim names to config urls.
skimConfig = {}




# Test SKIMS:

skimConfig["T1SkimTester"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"
skimConfig["RawT1SkimTester"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/rawprescaleskimmer.py?revision=1.2"

#addTier1Skim(tier0Config, "Skim1", "RECO", "Cosmics", defaultRecoVersion, "v5test",
#             skimConfig["T1SkimTester"], True)
#addTier1Skim(tier0Config, "Skim2", "RECO", "Calo", defaultRecoVersion, "v5test",
#             skimConfig["T1SkimTester"], True)
#addTier1Skim(tier0Config, "Skim3", "RECO", "MinimumBias", defaultRecoVersion, "v5test",
#             skimConfig["T1SkimTester"], True)
#addTier1Skim(tier0Config, "Skim4", "RECO", "MinimumBiasNoCalo", defaultRecoVersion, "v5test",
#             skimConfig["T1SkimTester"], True)
#addTier1Skim(tier0Config, "Skim5", "RECO", "BeamHalo", defaultRecoVersion, "v5test",
#             skimConfig["T1SkimTester"], True)
#addTier1Skim(tier0Config, "Skim6", "RAW", "RandomTriggers", defaultRecoVersion, "v5test",
#             skimConfig["RawT1SkimTester"], True)



# Used in production but now deprecated:


skimConfig["BeamHaloSkim"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/CSC_BeamHalo_cfg.py?revision=1.2"
skimConfig["MinBias2TrackSkim"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/RecoTrack_cfg.py?revision=1.1"
skimConfig["MinBias900SDSkim1"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/skim900GeV_StreamA_MinBiasPD.py?revision=1.1"
skimConfig["ZeroBias900SDSkim1"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/skim900GeV_StreamA_ZeroBiasPD.py?revision=1.1"

#addTier1Skim(tier0Config, "RealSkim1", "RECO", "MinimumBias", defaultRecoVersion, "PromptSkimCommissioning_v1",
#             skimConfig["BeamHaloSkim"], True)

#addTier1Skim(tier0Config, "RealSkim2", "RECO", "Cosmics", defaultRecoVersion, "PromptSkimCommissioning_v1",
#             skimConfig["BeamHaloSkim"], True)
#addTier1Skim(tier0Config, "RealSkim3", "RECO", "MinimumBias", defaultRecoVersion, "PromptSkimCommissioning_v1",
#             skimConfig["MinBias2TrackSkim"], True)
#addTier1Skim(tier0Config, "RealSkim4", "RECO", "MinimumBias", defaultRecoVersion, "PromptSkimCommissioning_v1",
#             skimConfig["MinBias900SDSkim1"], True)
#addTier1Skim(tier0Config, "RealSkim5", "RECO", "ZeroBias", defaultRecoVersion, "PromptSkimCommissioning_v1",
#             skimConfig["ZeroBias900SDSkim1"], True)


# Currently active

skimConfig["LucaMalgeri1"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/CosmicsPDSkim_cfg.py?revision=1.3"
skimConfig["LucaMalgeri2"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/MinBiasPDSkim_cfg.py?revision=1.3"
skimConfig["RobertoRossin1"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/skim900GeV_StreamA_ZeroBiasPD_cfg.py"
skimConfig["RobertoRossin2"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/skim900GeV_StreamA_MinBiasPD_cfg.py"

addTier1Skim(tier0Config, "RealSkim6", "RECO", "Cosmics", defaultRecoVersion, "PromptSkimCommissioning_v1",
             skimConfig["LucaMalgeri1"], True)

addTier1Skim(tier0Config, "RealSkim7", "RECO", "MinimumBias", defaultRecoVersion, "PromptSkimCommissioning_v1",
             skimConfig["LucaMalgeri2"], True)


addTier1Skim(tier0Config, "RealSkim8", "RECO", "ZeroBias", defaultRecoVersion, "PromptSkimCommissioning_v2",
             skimConfig["RobertoRossin1"], True)

addTier1Skim(tier0Config, "RealSkim9", "RECO", "MinimumBias", defaultRecoVersion, "PromptSkimCommissioning_v2",
             skimConfig["RobertoRossin2"], True)



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

#expressProcConfig["collision"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/expressCollisionReco_RAW2DIGI_RECO_DQM_ALCA.py?revision=1.6"

expressProcConfig["collision"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/expressCollisionReco_FirstCollisions_RAW2DIGI_RECO_DQM_ALCA.py?revision=1.4"


#expressProcConfig["hltmon"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/expressReco_RAW2DIGI_RECO_DQM_ALCA.py?revision=1.6"


# Create a dictionary that associated express merge packing config urls to names

expressMergePackConfig = {}
#expressMergePackConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_2_2_6/src/mergepacktestwithPrescales.py"
#expressMergePackConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_2_2_13/src/alCaRecoSplitting_express_cfg.py"

#For 31x
#expressMergePackConfig["default"] = "/data/cmsprod/CMSSW/CMSSW_3_1_0/src/recoAlcaMergePack.py"
expressMergePackConfig["default"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_express_cfg.py?revision=1.7"


expressProcConfig["alca"] = "/data/cmsprod/CMSSW/CMSSW_2_2_13/src/step23_EcalCalPi0Calib_ALCA_CRAFT_processing.py"
expressMergePackConfig["alca"] = "/data/cmsprod/CMSSW/CMSSW_2_2_13/src/step23_EcalCalPi0Calib_ALCA_CRAFT_alcamerge.py"




#addExpressConfig(tier0Config, "Express",
#                  expressProcConfig["default"],
#                  expressMergePackConfig["default"], False,defaultProcVersion)

addExpressConfig(tier0Config, "Express",
                 proc_config = expressProcConfig["collision"],
                 data_tiers = [ "FEVT", "ALCARECO", "DQM" ],
                 alcamerge_config = expressMergePackConfig["default"],
                 global_tag = expressGlobalTag,
                 splitInProcessing = True,
                 proc_ver = expressProcVersion)

addExpressConfig(tier0Config, "HLTMON",
                 proc_config = expressProcConfig["collision"],
                 data_tiers = [ "FEVTHLTALL", "DQM" ],
                 #data_tiers = [ "FEVT" ],
                 global_tag = expressGlobalTag,
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
setExpressVersionMapping(tier0Config, "CMSSW_3_3_5", "CMSSW_3_3_5_patch3")
setExpressVersionMapping(tier0Config, "CMSSW_3_3_4", "CMSSW_3_3_5_patch3")
setExpressVersionMapping(tier0Config, "CMSSW_3_3_3", "CMSSW_3_3_5_patch3")
setExpressVersionMapping(tier0Config, "CMSSW_3_2_7", "CMSSW_3_2_8")
setExpressVersionMapping(tier0Config, "CMSSW_3_2_4", "CMSSW_3_2_8")
setExpressVersionMapping(tier0Config, "CMSSW_3_2_3", "CMSSW_3_2_8")
setExpressVersionMapping(tier0Config, "CMSSW_3_2_2", "CMSSW_3_2_8")
setExpressVersionMapping(tier0Config, "CMSSW_3_2_1", "CMSSW_3_2_8")
setExpressVersionMapping(tier0Config, "CMSSW_3_1_1", "CMSSW_3_1_1_patch1")
setExpressVersionMapping(tier0Config, "CMSSW_3_1_0", "CMSSW_3_1_1_patch1")

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
#setRepackVersionMapping(tier0Config, "CMSSW_3_3_1", "CMSSW_6_6_6")
#setRepackVersionMapping(tier0Config, "CMSSW_3_3_0", "CMSSW_6_6_6")
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
