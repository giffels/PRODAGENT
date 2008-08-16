#!/usr/local/bin/bash

export STAGER_TRACE=3
export PRODAGENT_USER=cmsprod

#This next one is the first letter of your username
export PRODAGENT_INITIAL=c

#The version you want to install
export PAVERSION=0_11_7
#The DBS version we want to install
export DBSVERSION=1_2_3

#Note, don't change this!
#export PBIN=$HOME/public/bin/PRODAGENT_$PAVERSION
export PBIN=/data/cmsprod/PAProd/prod/control

#Don't change this!
export MYTESTAREA=/data/$PRODAGENT_USER/PAProd/prod/install
export PRODAGENT_WORKDIR=/data/$PRODAGENT_USER/PAProd/prod/prodAgent
export APT_VER=0.5.15lorg3.2-cmp
export VO_CMS_SW_DIR=$MYTESTAREA
export SCRAM_ARCH_INSTALL=slc4_amd64_gcc345
export LANG "C"

export CVSROOT=:kserver:$PRODAGENT_USER@cmscvs.cern.ch:/cvs/CMSSW 
export PRODAGENT_CONFIG=$PBIN/RepackerConfig.xml


#This variable defines the castor pool used for I/O.
#It should match the pool where the input data was staged (check with Miller, Ceballos)
#For MC you usually want pool cmsprod
#For dedicated reco jobs (mostly scale tests), use t0export instead
#If you are unsure, contact Markus Klute and Mike Miller

#export STAGE_SVCCLASS=cmsprod 
export STAGE_SVCCLASS=t0export
