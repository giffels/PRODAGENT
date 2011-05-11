#!/bin/bash

#make directory
mkdir -p $PRODAGENT_WORKDIR

#cleanup old install
rm -rf $MYTESTAREA/PRODAGENT
cd $MYTESTAREA

cvs co -r PRODAGENT_$PAVERSION PRODAGENT

# Place any PA patches here -- as in 
cvs upd -r 1.13 PRODAGENT/src/python/StageOut/Impl/RFCPCERNImpl.py 
cvs upd -r 1.27 PRODAGENT/src/python/JobCreator/RuntimeTools/RuntimeOfflineDQM.py 
cvs upd -r 1.60 PRODAGENT/src/python/JobCreator/JobCreatorComponent.py 

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

# T0 Patches here
cvs upd -r 1.52 T0/src/python/T0/WorkflowFactory/FactoryInterface.py

cvs co -r WMCORE_$WMCOREVERSION WMCORE
 
# place any WMCORE patches here:


cd WMCORE
make
cd $MYTESTAREA
