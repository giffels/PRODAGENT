#!/usr/bin/env python
"""
_OfflineConfiguration_

Processing configuration for the Tier0.
"""

__revision__ = "$Id: OfflineConfiguration.py,v 1.23 2009/12/05 17:35:21 dmason Exp $"
__version__ = "$Revision: 1.23 $"

from T0.RunConfigCache.Tier0Config import addRepackConfig
from T0.RunConfigCache.Tier0Config import addDataset
from T0.RunConfigCache.Tier0Config import addTier1Skim
from T0.RunConfigCache.Tier0Config import addExpressConfig
from T0.RunConfigCache.Tier0Config import createTier0Config
from T0.RunConfigCache.Tier0Config import setAcquisitionEra
from T0.RunConfigCache.Tier0Config import setConfigVersion

# Create the Tier0 configuration object
tier0Config = createTier0Config()

# Set global parameters like the acquisition era and the version of
# the configuration.
setAcquisitionEra(tier0Config, "EXCITING_T0TEST_WITHTOTALLYDIFFERENTLOOKINGBUNNIES")
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


defaultRecoVersion = "CMSSW_3_5_2"
defaultAlcaVersion = defaultRecoVersion
defaultDQMVersion = defaultRecoVersion


defaultProcVersion = "v10"
expressProcVersion = "v10"
recoProcVersion = "v10"

#35X
expressGlobalTag = "GR10_E_V2::All"
promptrecoGlobalTag = "GR10_P_V2::All"
cosmicsrecoGlobalTag = "GR10_P_V2COS::All"

defaultRecoSplitting = 20000
alcaRecoSplitting = 100000

#
# Create a dictionary that associates a reco configuration with a scenario.
# The configuration must be specified as a url.
recoConfig = {}
alcaConfig = {}

#cosmics PromptReco config with combined alca
recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/promptReco_RAW2DIGI_RECO_DQM_ALCA.py?revision=1.14"

#collision PromptReco config with combined alca
recoConfig["collision"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/promptCollisionReco_FirstCollisions_RAW2DIGI_L1Reco_RECO_DQM_ALCA.py?revision=1.11"

#HcalNZS PromptReco config
recoConfig["hcalnzs"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/promptCollisionNZSReco_RAW2DIGI_L1Reco_RECO_DQM_ALCA.py?revision=1.3"


#promptreco configs for alcaraw datasets
recoConfig["AlCaP0"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alcareco_AlCaP0_cfg.py?revision=1.3"
recoConfig["AlCaPhiSymEcal"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alcareco_AlCaPhiSymEcal_cfg.py?revision=1.2"
recoConfig["AlCaPhiSymHcal"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alcareco_AlCaPhiSymHcal_cfg.py?revision=1.2"

#Alca splitting configs for PromptReco
alcaConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_Cosmics_cfg.py?revision=1.2"
alcaConfig["calo"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_prompt_Calo_cfg.py?revision=1.3"
alcaConfig["minbias"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_MinimumBias_cfg.py?revision=1.3"

#for alcaraw streams
alcaConfig["AlCaP0"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_AlCaP0_cfg.py?revision=1.4"
alcaConfig["AlCaPhiSymEcal"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_AlCaPhiSymEcal_cfg.py?revision=1.3"
alcaConfig["AlCaPhiSymHcal"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_AlCaPhiSymHcal_cfg.py?revision=1.4"

#define repack and express version remappings if needed

repackVersionOverride = {
                        }

expressVersionOverride = {
                           "CMSSW_3_5_0" : "CMSSW_3_5_2",
                           "CMSSW_3_5_1" : "CMSSW_3_5_2"
                         }


#set default repack settings for bulk streams
addRepackConfig(tier0Config, "Default",
                proc_ver = defaultProcVersion,
                versionOverride = repackVersionOverride)

#set default dataset config (repack only and subscription to T0_CH_CERN_MSS only)
addDataset(tier0Config, "Default",
           scenario = "pp",
           default_proc_ver = defaultProcVersion,
           archival_node = "T0_CH_CERN_MSS")


# PD-specific configuration

addDataset(tier0Config, "BeamHalo",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_split = defaultRecoSplitting,
           reco_configuration = recoConfig["cosmics"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["cosmics"],
#           custodial_node = "T1_FR_CCIN2P3_MSS",
#           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS"
           )
addDataset(tier0Config, "Calo",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_split = defaultRecoSplitting,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
#           custodial_node = "T1_DE_KIT_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "Cosmics",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = cosmicsrecoGlobalTag,
           reco_split = defaultRecoSplitting,
           reco_configuration = recoConfig["cosmics"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = True, alca_version= defaultAlcaVersion, 
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["cosmics"],
#           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS"
           )
addDataset(tier0Config, "HcalNZS",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_split = defaultRecoSplitting,
           reco_configuration = recoConfig["hcalnzs"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = True, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["AlCaPhiSymHcal"],
#           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS"
           )
addDataset(tier0Config, "MinimumBias",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_split = defaultRecoSplitting,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = True, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["minbias"],
#           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "MinimumBiasNoCalo",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_split = defaultRecoSplitting,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
#           custodial_node = "T1_ES_PIC_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "PhysicsMuonBkg",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_split = defaultRecoSplitting,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultRecoVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
#           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "ZeroBias",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_split = defaultRecoSplitting,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = True, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
#           custodial_node = "T1_IT_CNAF_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "ZeroBiasB",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = False, global_tag = promptrecoGlobalTag,
           reco_split = defaultRecoSplitting,
           reco_proc_ver = recoProcVersion,
           reco_configuration = recoConfig["collision"],
           reco_version = defaultRecoVersion,
           do_dqm = True,dqm_version=defaultDQMVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["calo"],
#           custodial_node = "T1_DE_KIT_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config, "ZeroBiasBnotT0",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = False, global_tag = promptrecoGlobalTag,
           reco_split = defaultRecoSplitting,
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
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = False, global_tag = promptrecoGlobalTag,
#           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"RandomTriggers",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = False, global_tag = promptrecoGlobalTag,
#           custodial_node = "T1_ES_PIC_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"AlCaP0",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_split = alcaRecoSplitting,
           reco_configuration = recoConfig["AlCaP0"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_alca = True, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["AlCaP0"],
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"AlCaPhiSymEcal",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = True, global_tag = promptrecoGlobalTag,
           reco_split = alcaRecoSplitting,
           reco_configuration = recoConfig["AlCaPhiSymEcal"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_alca = True, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["AlCaPhiSymEcal"],
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"AlCaPhiSymHcal",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = False, global_tag = promptrecoGlobalTag,
           reco_split = alcaRecoSplitting,
           reco_configuration = recoConfig["AlCaPhiSymHcal"],
           reco_proc_ver = recoProcVersion,
           reco_version = defaultRecoVersion,
           do_alca = False, alca_version= defaultAlcaVersion,
           alca_proc_ver = recoProcVersion,
           alca_configuration=alcaConfig["AlCaPhiSymHcal"],
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"TestEnables",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = False, global_tag = promptrecoGlobalTag,
#           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"LogMonitor",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = False, global_tag = promptrecoGlobalTag,
#           custodial_node = "T1_US_FNAL_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"RPCMonitor",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = False, global_tag = promptrecoGlobalTag,
#           custodial_node = "T1_ES_PIC_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"FEDMonitor",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = False, global_tag = promptrecoGlobalTag,
#           custodial_node = "T1_ES_PIC_MSS",
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"RandomTriggersOpen",
           default_proc_ver = defaultProcVersion, scenario = "pp",
           do_reco = False, global_tag = promptrecoGlobalTag,
           archival_node = "T0_CH_CERN_MSS")
addDataset(tier0Config,"Test",
           default_proc_ver = defaultProcVersion, scenario = "pp",
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

#Express cosmics config
expressProcConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/expressReco_RAW2DIGI_RECO_DQM_ALCA.py?revision=1.3" 

#Express collision config
expressProcConfig["collision"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/expressCollisionReco_FirstCollisions_RAW2DIGI_RECO_DQM_ALCA.py?revision=1.11"


#expressProcConfig["hltmon"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/expressReco_RAW2DIGI_RECO_DQM_ALCA.py?revision=1.6"


# Create a dictionary that associated express merge packing config urls to names

expressMergePackConfig = {}

#Express Alca splitting config
expressMergePackConfig["default"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/alCaRecoSplitting_express_cfg.py?revision=1.9"

#configure express processing per stream
addExpressConfig(tier0Config, "Express",
                 scenario = "pp",
                 proc_config = expressProcConfig["collision"],
                 data_tiers = [ "FEVT", "ALCARECO", "DQM" ],
                 alcamerge_config = expressMergePackConfig["default"],
                 global_tag = expressGlobalTag,
                 splitInProcessing = True,
                 proc_ver = expressProcVersion,
                 versionOverride = expressVersionOverride)

addExpressConfig(tier0Config, "HLTMON",
                 scenario = "pp",
                 proc_config = expressProcConfig["collision"],
                 data_tiers = [ "FEVTHLTALL", "DQM" ],
                 global_tag = expressGlobalTag,
                 splitInProcessing = True,
                 proc_ver = expressProcVersion,
                 versionOverride = expressVersionOverride)

if __name__ == '__main__':
    print tier0Config
