#!/bin/bash

export STAGER_TRACE=3
export PRODAGENT_USER=cmsprod

# usually set to either dev, int, or if you like gunshot wounds, prod
export INSTALL_TYPE=test2

#This next one is the first letter of your username
export PRODAGENT_INITIAL=c

#PA RPM version -- this controls the dependencies

#export PARPMS=0_12_16_patch2-cmp
export RPMVERSION=0_12_17_pre5

# these are all parts of tags needed to check out the actual codebase from CVS
# overriding what comes with the RPMS

#The ProdAgent & ProdCommon versions you want to pull out of CVS
export PAVERSION=0_12_17_pre5
export PCVERSION=0_12_17_pre5
#The DBS version we want to install
export DBSVERSION=2_0_9_patch_4
#The WMCORE version required
export WMCOREVERSION=T0_1_0_0_pre10
#The CASTOR version, probably doesn't change much
export CASTORVERSION=2.1.7.14-cmp
#The T0 version we need
export T0VERSION=1_0_0_pre10

# This is probably the only one to change
export PRODAGENT_TOP=/data/$PRODAGENT_USER/PAProd/$INSTALL_TYPE

#Note, don't change this!
#export PBIN=$HOME/public/bin/PRODAGENT_$PAVERSION
export PBIN=${PRODAGENT_TOP}/control

#Don't change this!
export MYTESTAREA=${PRODAGENT_TOP}/install
export PRODAGENT_WORKDIR=${PRODAGENT_TOP}/prodAgent
export APT_VER=0.5.15lorg3.2-cmp
export VO_CMS_SW_DIR=$MYTESTAREA
export SCRAM_ARCH_INSTALL=slc5_amd64_gcc434
export LANG "C"

export CVSROOT=:gserver:$PRODAGENT_USER@cmscvs.cern.ch:/cvs/CMSSW 
export PRODAGENT_CONFIG=$PBIN/RepackerConfig.xml


#This variable defines the castor pool used for I/O.
#It should match the pool where the input data was staged (check with Miller, Ceballos)
#For MC you usually want pool cmsprod
#For dedicated reco jobs (mostly scale tests), use t0export instead
#If you are unsure, contact Markus Klute and Mike Miller

#export STAGE_SVCCLASS=cmsprod 
# t0input for replays only!!!  doesn't write to tape
#export STAGE_SVCCLASS=t0input
export STAGE_SVCCLASS=t0export
