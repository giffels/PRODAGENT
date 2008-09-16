#!/usr/local/bin/bash

export PRODAGENT_USER=relval

#This next one is the first letter of your username
export PRODAGENT_INITIAL=r

#The version you want to install
export PAVERSION=0_11_7
#The DBS version we want to install
export DBSVERSION=1_2_8
#PRODCOMMON Version
export PCVERSION=0_11_5

#The architecture you want to use
#export SCRAM_ARCH=slc4_amd64_gcc345
export SCRAM_ARCH=slc4_ia32_gcc345  # Necessary for gLite

#The language
export LANG "C"

#Inserted (12.08.2008)
export STAGER_TRACE=3

#The type of instalation you want to perform (for using LSF submission at the T0 or LCG submission to the grid)
#and the site where you want to run your LCG jobs (LSF has to use CERN!)
#export RUN_SITE=CERN; export INST_TYPE=T0LSF;
export RUN_SITE=FNAL; export INST_TYPE=LCG;
#export RUN_SITE=MIT; export INST_TYPE=LCG; 

#Note, don't change this!
export PBIN="/data/$PRODAGENT_USER/PAProd/$RUN_SITE-$INST_TYPE-$PAVERSION"

#Don't change this!
export MYTESTAREA="/data/$PRODAGENT_USER/PAProd/$RUN_SITE-$INST_TYPE-$PAVERSION/install"
export PRODAGENT_WORKDIR="/data/$PRODAGENT_USER/PAProd/$RUN_SITE-$INST_TYPE-$PAVERSION/prodAgent"
export PRODAGENT_CONFIG=$PBIN/ProdAgentConfig.xml
export CVSROOT=:kserver:relval@cmscvs.cern.ch:/cvs_server/repositories/CMSSW 


#This variable defines the castor pool used for I/O.
#It should match the pool where the input data was staged (check with Miller, Ceballos)
#For MC you usually want pool cmsprod
#For dedicated reco jobs (mostly scale tests), use t0export instead
#If you are unsure, contact Markus Klute and Mike Miller

export STAGE_SVCCLASS=cmsprod 
#export STAGE_SVCCLASS=t0export
#export STAGE_SVCCLASS=cmscaf
