#!/bin/bash

thisDir=`pwd` 

#make directory
mkdir -p $PRODAGENT_WORKDIR
#make PhEDEx Dropbox directory
mkdir -p $PRODAGENT_WORKDIR/PhEDExDrop

#cleanup old install
rm -rf $MYTESTAREA/PRODAGENT
rm -rf $MYTESTAREA/PRODCOMMON
rm -rf $MYTESTAREA/DBS
cd $MYTESTAREA

#Updating PRODAGENT from cvs
cvs co -r PRODAGENT_$PAVERSION PRODAGENT
sleep 3
cd PRODAGENT
make
#Updating PRODCOMMON from cvs
cd $MYTESTAREA
cvs co -r PRODCOMMON_$PCVERSION PRODCOMMON
sleep 3
cd PRODCOMMON
make
#Updating DBS from cvs
cd $MYTESTAREA
cvs co -r DBS_$DBSVERSION DBS

#source $PBIN/prodagent_init.sh
source $PBIN/env_mytestarea.sh;


source $MYTESTAREA/$SCRAM_ARCH/cms/prodagent/PRODAGENT_$PAVERSION-cmp/etc/profile.d/dependencies-setup.sh


export PRODAGENT_ROOT=$MYTESTAREA/PRODAGENT
export PATH=$PRODAGENT_ROOT/bin:$PATH
export PUTIL=$PRODAGENT_ROOT/util

export DBS_ROOT=$MYTESTAREA/DBS

#First verify that you do not have to update PRODCOMMON from cvs
export PRODCOMMON_ROOT=$MYTESTAREA/PRODCOMMON
export PYTHONPATH=$PRODAGENT_ROOT/lib:$PRODCOMMON_ROOT/lib:$DBS_ROOT/Clients/Python:$PYTHONPATH
#export PYTHONPATH=$PRODAGENT_ROOT/lib:$DBS_ROOT/Clients/Python

#and here we hack the DBS env for chain processing
#export DBS_CLIENT_CONFIG=$DBS_ROOT/Clients/Python/DBSAPI/dbs.config


#Editing __init__.py script in order to avoid importing JobEmulator (0_11_0)
#if [ "$PAVERSION" == "0_11_0" -o "$PAVERSION" == "0_11_3" ]; then
#	cat $MYTESTAREA/PRODAGENT/src/python/JobCreator/Creators/__init__.py | sed s/"import JobEmulatorCreator"/"#import JobEmulatorCreator"/ > $MYTESTAREA/PRODAGENT/src/python/JobCreator/Creators/__init__.py_temp
#	rm $MYTESTAREA/PRODAGENT/src/python/JobCreator/Creators/__init__.py
#	mv $MYTESTAREA/PRODAGENT/src/python/JobCreator/Creators/__init__.py_temp $MYTESTAREA/PRODAGENT/src/python/JobCreator/Creators/__init__.py
#fi

cd $thisDir
