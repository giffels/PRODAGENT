#!/bin/sh

python ./util/resourceControl.py --new --site=FNAL  --se-name=cmssrm.fnal.gov           --ce-name=none --activate
python ./util/resourceControl.py --new --site=IN2P3 --se-name=ccsrm.in2p3.fr            --ce-name=none --activate
python ./util/resourceControl.py --new --site=RAL   --se-name=srm-cms.gridpp.rl.ac.uk   --ce-name=none --activate
python ./util/resourceControl.py --new --site=FZK   --se-name=gridka-dCache.fzk.de      --ce-name=none --activate
python ./util/resourceControl.py --new --site=PIC   --se-name=srmcms.pic.es             --ce-name=none --activate
python ./util/resourceControl.py --new --site=CNAF  --se-name=srm-v2-cms.cr.cnaf.infn.it --ce-name=none --activate
python ./util/resourceControl.py --new --site=ASGC  --se-name=srm2.grid.sinica.edu.tw --ce-name=none --activate

python ./util/resourceControl.py --edit --site=FNAL --set-threshold=mergeThreshold --value=500
python ./util/resourceControl.py --edit --site=FNAL --set-threshold=mergeRunningThrottle --value=2000
python ./util/resourceControl.py --edit --site=FNAL --set-threshold=processingThreshold --value=1000
python ./util/resourceControl.py --edit --site=FNAL --set-threshold=processingRunningThrottle --value=7000
python ./util/resourceControl.py --edit --site=FNAL --set-threshold=maximumSubmission --value=500
python ./util/resourceControl.py --edit --site=FNAL --set-threshold=minimumSubmission --value=1
python ./util/resourceControl.py --edit --site=FNAL --set-threshold=cleanupThreshold --value=50

python ./util/resourceControl.py --edit --site=IN2P3 --set-threshold=mergeThreshold --value=50
python ./util/resourceControl.py --edit --site=IN2P3 --set-threshold=mergeRunningThrottle --value=300
python ./util/resourceControl.py --edit --site=IN2P3 --set-threshold=processingThreshold --value=200  
python ./util/resourceControl.py --edit --site=IN2P3 --set-threshold=processingRunningThrottle --value=900  
python ./util/resourceControl.py --edit --site=IN2P3 --set-threshold=maximumSubmission --value=200 
python ./util/resourceControl.py --edit --site=IN2P3 --set-threshold=minimumSubmission --value=1 
python ./util/resourceControl.py --edit --site=IN2P3 --set-threshold=cleanupThreshold --value=20

python ./util/resourceControl.py --edit --site=RAL --set-threshold=mergeThreshold --value=100
python ./util/resourceControl.py --edit --site=RAL --set-threshold=mergeRunningThrottle --value=200
python ./util/resourceControl.py --edit --site=RAL --set-threshold=processingThreshold --value=100
python ./util/resourceControl.py --edit --site=RAL --set-threshold=processingRunningThrottle --value=350
python ./util/resourceControl.py --edit --site=RAL --set-threshold=maximumSubmission --value=200
python ./util/resourceControl.py --edit --site=RAL --set-threshold=minimumSubmission --value=1
python ./util/resourceControl.py --edit --site=RAL --set-threshold=cleanupThreshold --value=20

python ./util/resourceControl.py --edit --site=FZK --set-threshold=mergeThreshold --value=50
python ./util/resourceControl.py --edit --site=FZK --set-threshold=mergeRunningThrottle --value=200
python ./util/resourceControl.py --edit --site=FZK --set-threshold=processingThreshold --value=100
python ./util/resourceControl.py --edit --site=FZK --set-threshold=processingRunningThrottle --value=600
python ./util/resourceControl.py --edit --site=FZK --set-threshold=maximumSubmission --value=200
python ./util/resourceControl.py --edit --site=FZK --set-threshold=minimumSubmission --value=1
python ./util/resourceControl.py --edit --site=FZK --set-threshold=cleanupThreshold --value=20

python ./util/resourceControl.py --edit --site=CNAF --set-threshold=mergeThreshold --value=50
python ./util/resourceControl.py --edit --site=CNAF --set-threshold=mergeRunningThrottle --value=200
python ./util/resourceControl.py --edit --site=CNAF --set-threshold=processingThreshold --value=100
python ./util/resourceControl.py --edit --site=CNAF --set-threshold=processingRunningThrottle --value=600
python ./util/resourceControl.py --edit --site=CNAF --set-threshold=maximumSubmission --value=200
python ./util/resourceControl.py --edit --site=CNAF --set-threshold=minimumSubmission --value=1
python ./util/resourceControl.py --edit --site=CNAF --set-threshold=cleanupThreshold --value=20

python ./util/resourceControl.py --edit --site=PIC --set-threshold=mergeThreshold --value=50
python ./util/resourceControl.py --edit --site=PIC --set-threshold=mergeRunningThrottle --value=100
python ./util/resourceControl.py --edit --site=PIC --set-threshold=processingThreshold --value=50
python ./util/resourceControl.py --edit --site=PIC --set-threshold=processingRunningThrottle --value=200
python ./util/resourceControl.py --edit --site=PIC --set-threshold=maximumSubmission --value=20
python ./util/resourceControl.py --edit --site=PIC --set-threshold=minimumSubmission --value=1
python ./util/resourceControl.py --edit --site=PIC --set-threshold=cleanupThreshold --value=10

python ./util/resourceControl.py --edit --site=ASGC --set-threshold=mergeThreshold --value=50
python ./util/resourceControl.py --edit --site=ASGC --set-threshold=mergeRunningThrottle --value=150
python ./util/resourceControl.py --edit --site=ASGC --set-threshold=processingThreshold --value=600
python ./util/resourceControl.py --edit --site=ASGC --set-threshold=processingRunningThrottle --value=1350
python ./util/resourceControl.py --edit --site=ASGC --set-threshold=maximumSubmission --value=200
python ./util/resourceControl.py --edit --site=ASGC --set-threshold=minimumSubmission --value=1
python ./util/resourceControl.py --edit --site=ASGC --set-threshold=cleanupThreshold --value=20


python ./util/resourceControl.py --list
