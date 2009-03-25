#!/usr/local/bin/bash

export STAGER_TRACE=3
export PRODAGENT_USER=cmsprod

# usually set to either dev, int, or if you like gunshot wounds, prod
export INSTALL_TYPE=dev

#This next one is the first letter of your username
export PRODAGENT_INITIAL=c

#PA RPM version -- this controls the dependencies
export RPMVERSION=0_12_13

# these are all parts of tags needed to check out the actual codebase from CVS
# overriding what comes with the RPMS

#The ProdAgent & ProdCommon versions you want to pull out of CVS
export PAVERSION=0_12_13
export PCVERSION=0_12_13
#The DBS version we want to install
export DBSVERSION=2_0_5
#The WMCORE version required
export WMCOREVERSION=T0_0_0_2_pre58a
#The T0 version we need
export T0VERSION=0_0_2_pre57

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
export SCRAM_ARCH_INSTALL=slc4_amd64_gcc345
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
