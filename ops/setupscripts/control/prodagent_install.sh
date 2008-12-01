#!/usr/local/bin/bash

#make directory
mkdir -p $PRODAGENT_WORKDIR

#cleanup old install
rm -rf $MYTESTAREA/PRODAGENT
cd $MYTESTAREA

cvs co -r PRODAGENT_$PAVERSION PRODAGENT


# follwoing twiki at https://twiki.cern.ch/twiki/bin/view/CMS/CMST0Repacker:
cvs update -r 1.8 PRODAGENT/src/python/DQMInjector/Plugins/T0ASTPlugin.py

cd PRODAGENT
source $PBIN/prodagent_init.sh
make


rm -rf lib/RepackerInjector

# again for PRODCOMMON

cd $MYTESTAREA
cvs co -r PRODCOMMON_$PCVERSION PRODCOMMON

# follwoing twiki at https://twiki.cern.ch/twiki/bin/view/CMS/CMST0Repacker:
cvs update -r 1.15 PRODCOMMON/src/python/ProdCommon/MCPayloads/WorkflowSpec.py 
cvs update -r 1.9 PRODCOMMON/src/python/ProdCommon/MCPayloads/JobSpecNode.py

cd PRODCOMMON
make

cd $MYTESTAREA
# pull the DBS client out of DBS
cvs co -r DBS_$DBSVERSION DBS

# T0 specific components

cvs co -r T0_$T0VERSION T0
cvs co -r WMCORE_$WMCOREVERSION WMCORE
cvs co -r LUMIDB_0_0_2 LUMIDB/LumiWebService/Client/LumiWebClient

