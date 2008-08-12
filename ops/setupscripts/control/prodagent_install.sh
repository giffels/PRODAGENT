#!/usr/local/bin/bash

#make directory
mkdir -p $PRODAGENT_WORKDIR

#cleanup old install
rm -rf $MYTESTAREA/PRODAGENT
cd $MYTESTAREA

cvs co -r PRODAGENT_$PAVERSION PRODAGENT

cd PRODAGENT
source $PBIN/prodagent_init.sh
make


rm -rf lib/RepackerInjector

# again for PRODCOMMON

cd $MYTESTAREA
cvs co -r PRODCOMMON_$PAVERSION PRODCOMMON
cd PRODCOMMON
make

cd $MYTESTAREA
# pull the DBS client out of DBS
cvs co -r DBS_$DBSVERSION DBS

# T0 specific components

cvs co -r T0_0_0_2_pre13 T0
cvs co -r LUMIDB_0_0_2 LUMIDB/LumiWebService/Client/LumiWebClient

