#!/bin/bash

#make directory
mkdir -p $PRODAGENT_WORKDIR

#cleanup old install
rm -rf $MYTESTAREA/PRODAGENT
cd $MYTESTAREA

cvs co -r PRODAGENT_$PAVERSION PRODAGENT

# Place any PA patches here -- as in 


echo PA CVS updates done...
sleep 5

cd PRODAGENT
source $PBIN/prodagent_init.sh
make


rm -rf lib/RepackerInjector

# again for PRODCOMMON

cd $MYTESTAREA
cvs co -r PRODCOMMON_$PCVERSION PRODCOMMON

# place any PC patches here:


echo PC CVS updates done...
sleep 5
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

cvs upd -r 1.59 T0/src/python/T0/AlcaSkimInjector/AlcaSkimInjectorComponent.py
cvs upd -r 1.25 T0/src/python/T0/WorkflowFactory/ExpressWorkflow.py
cvs upd -r 1.56 T0/src/python/T0/State/Database/Reader/ListFiles.py
cvs upd -r 1.41 T0/src/python/T0/RepackerAuditor/RepackerAuditorComponent.py
cvs upd -r 1.50 T0/src/python/T0/Tier0Merger/Tier0MergerComponent.py

cvs co -r WMCORE_$WMCOREVERSION WMCORE
 
# place any WMCORE patches here:


cd WMCORE
make
cd $MYTESTAREA


cvs co -r LUMIDB_0_0_2 LUMIDB/LumiWebService/Client/LumiWebClient

