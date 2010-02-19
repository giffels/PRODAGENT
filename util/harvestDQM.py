#!/usr/bin/env python
"""
_harvestDQM_

Util for publishing a collection trigger event for a dataset/run

"""

import sys
import getopt

from MessageService.MessageService import MessageService
from DQMInjector.CollectPayload import CollectPayload


valid = ['run=', 'primary=', 'processed=', 'tier=' , 'scenario=', 'path=',
         'tag=']


usage = \
"""
Usage:

harvestDQM.py --run=<RUN>
              --primary=<PRIMARYDS>
              --processed=<PROCDS>
              --tier=<TIER>
              --scenario=<SCENARIO>
              --path=<DATASET_PATH>
              --plugin=<DQM_PLUGIN>
              --tag=<GLOBAL_TAG>

Details:

<RUN>: 
    Run Number to harvest. Overwrites <RUN> to 1 if --scenario=relvalmc.
<PRIMARYDS>: 
    Primary dataset name. Ignored if --dataset is provided.
<PROCDS>:
    Processed dataset name. Ignored if --dataset is provided.
<TIER>:
    Data tier. Ignored if --dataset is provided.
<SCENARIO>:
    Harvesting scenario. The following options are available:
    - cosmics: Cosmics data scenario
    - relvalmc: MC/RelVal scenario.
    - relvalmcfs: RelVal FastSim scenario.
<DATASET_PATH>:
    Full dataset path (/<PRIMARYDS>/<PROCDS>/<TIER>). Overwrittes <PRIMARYDS>,
    <PROCDS> and <TIER>.
<DQM_PLUGIN>:
    DQMInjector plugin to be used. Options are:
    - DBSPlugin
    - RelValPlugin
    - T0ASTPlugin
<GLOBAL_TAG>:
    Optional paramenter that allows to select the Global Tag. It should be
    complete, i.e. MC_31X_V9::All, CRAFT0831X_V3::All

"""

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

collect = CollectPayload()

#Default values
for key in collect.keys():
    collect[key] = None
path = None
plugin = None

valid_plugins = ('DBSPlugin', 'RelValPlugin', 'T0ASTPlugin')

for opt, arg in opts:
    if opt == "--run":
        collect['RunNumber'] = arg
    if opt == "--primary":
        collect['PrimaryDataset'] = arg
    if opt == "--processed":
        collect['ProcessedDataset'] = arg
    if opt == "--tier":
        collect['DataTier'] = arg
    if opt == "--scenario":
        collect['Scenario'] = arg
    if opt == "--tag":
        collect['GlobalTag'] = arg
    if opt == "--path":
        path = arg
    if opt == "--plugin":
        plugin = arg

if path is not None and len(path.split("/")) != 4 and not path.startswith('/'):
    msg = 'Invalid dataset path provided. '
    msg += 'It should be like /<PRIMARYDS>/<PROCDS>/<TIER>'
    print usage
    print msg
    sys.exit(1)
elif path is not None:
    dataset_parts = [x for x in path.split('/') if x]
    collect['PrimaryDataset'] = dataset_parts[0]
    collect['ProcessedDataset'] = dataset_parts[1]
    collect['DataTier'] = dataset_parts[2]
elif collect['PrimaryDataset'] is None \
    or collect['ProcessedDataset'] is None or \
    collect['DataTier'] is None:
    msg = 'You should provide either --path or --primary, --processed and --tier'
    print usage
    print msg
    sys.exit(1)

if collect['Scenario'] is None:
    msg = 'Scenario not provided.'
    print usage
    print msg
    sys.exit(1)
if collect['RunNumber'] is None:
    msg = 'You should provide --run.'
    print usage
    print msg
    sys.exit(1)

if plugin is not None and plugin not in valid_plugins:
    msg = 'Invalid plugin.'
    print usage
    print msg
    sys.exit(1)

ms = MessageService()
ms.registerAs("CLI")

if plugin is not None:
    print "Changing DQMinjector plugin to %s" % plugin
    ms.publish("DQMInjector:SetPlugin", str(plugin))
    ms.commit()

print "Publishing DQM workflow creation: %s" % str(collect)

ms.publish("DQMInjector:Collect", str(collect))
ms.commit()

