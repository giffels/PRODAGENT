#!/bin/sh

nohup python $WTBASE/python/WMCore/WebTools/Root.py --ini=/data/cmsprod/PAProd/proddqm/control/DASConfig.py &> DASlog.log &
