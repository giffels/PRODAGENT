#!/usr/local/bin/bash

#make directory
mkdir -p $PRODAGENT_WORKDIR

#cleanup old install
rm -rf $MYTESTAREA/PRODAGENT
cd $MYTESTAREA

cvs co -r PRODAGENT_$PAVERSION PRODAGENT

# follwoing twiki at https://twiki.cern.ch/twiki/bin/view/CMS/CMST0Repacker:

cvs update -r 1.10 PRODAGENT/src/python/ResourceMonitor/ResourceMonitorComponent.py
cvs update -r 1.12 PRODAGENT/src/python/ResourceMonitor/Monitors/T0LSFMonitor.py


cd PRODAGENT
source $PBIN/prodagent_init.sh
make


rm -rf lib/RepackerInjector

# again for PRODCOMMON

cd $MYTESTAREA
cvs co -r PRODCOMMON_$PCVERSION PRODCOMMON
cd PRODCOMMON
make

cd $MYTESTAREA
# pull the DBS client out of DBS
cvs co -r DBS_$DBSVERSION DBS

# T0 specific components

cvs co -r T0_0_0_2_pre23 T0
#cvs co -r WMCORE_REQMGR_0_0_1 WMCORE
cvs co -r LUMIDB_0_0_2 LUMIDB/LumiWebService/Client/LumiWebClient

