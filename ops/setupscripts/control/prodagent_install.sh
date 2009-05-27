#!/usr/local/bin/bash

#make directory
mkdir -p $PRODAGENT_WORKDIR

#cleanup old install
rm -rf $MYTESTAREA/PRODAGENT
cd $MYTESTAREA

cvs co -r PRODAGENT_$PAVERSION PRODAGENT

# Place any PA patches here -- as in 
# cvs update -r 1.8 PRODAGENT/src/python/DQMInjector/Plugins/T0ASTPlugin.py

cvs update -r 1.12 PRODAGENT/src/python/DQMInjector/Plugins/T0ASTPlugin.py 
cvs update -r 1.30 PRODAGENT/src/python/JobQueue/JobQueueDB.py
cvs update -A PRODAGENT/bin/prodAgentd

cd PRODAGENT
source $PBIN/prodagent_init.sh
make


rm -rf lib/RepackerInjector

# again for PRODCOMMON

cd $MYTESTAREA
cvs co -r PRODCOMMON_$PCVERSION PRODCOMMON

# place any PC patches here:


cvs update -r 1.12 PRODCOMMON/src/python/ProdCommon/FwkJobRep/FileInfo.py
cvs update -r 1.12 PRODCOMMON/src/python/ProdCommon/FwkJobRep/TaskState.py

cd PRODCOMMON
make

cd $MYTESTAREA

if [ ! -z $DBSVERSION ]; then

  # pull the DBS client out of DBS
  cvs co -r DBS_$DBSVERSION DBS
fi

if [ ! -z $DLSVERSION ]; then
  #pull the DLS client out of CVS
  cvs co -r DLS_$DLSVERSION DLS/Client
  cd DLS/Client
  make
  cd $MYTESTAREA
fi

# T0 specific components

cvs co -r T0_$T0VERSION T0

cvs update -A T0/src/python/T0/RunConfigCache/RunConfig.py 

cvs co -r WMCORE_$WMCOREVERSION WMCORE
 
# place any WMCORE patches here:


cd WMCORE
make
cd $MYTESTAREA


cvs co -r LUMIDB_0_0_2 LUMIDB/LumiWebService/Client/LumiWebClient

