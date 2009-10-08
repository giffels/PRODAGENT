#!/usr/bin/env python

import sys
import os
import getopt
import popen2

def main(argv) :
    """
    
    prepareRelValworkflows
    
    prepare workflows for chained processing of RelVal samples

    - parse file holding cmsDriver commands for 1st and 2nd steps
    - prepare workflows
    - prepare WorkflowInjector:Input script
    - prepare ForceMerge script
    - prepare DBSMigrationToGlobal script
    - prepare PhEDExInjection script
    - prepare local DBS query script
    
    required parameters
    --samples <textfile>            : list of RelVal sample parameter-sets in plain text file, one sample per line, # marks comment
    --version <processing version>  : processing version (v1, v2, ... )
    --DBSURL <URL>                  : URL of the local DBS (http://cmsdbsprod.cern.ch/cms_dbs_prod_local_07/servlet/DBSServlet | http://cmssrv46.fnal.gov:8080/DBSFNALT1206/servlet/DBSServlet)
    --only-sites                    : Site where dataset is going to be processed or where the input dataset is taken from. Usually srm-cms.cern.ch and cmssrm.fnal.gov
    
    optional parameters
    --pileupdataset                 : input pileup dataset. It must be provided if the <samples> txt file contains PilepUp samples
    --lumi <number>                 : initial run for generation (default: 666666), set it to 777777 for high statistics samples
    --event <number>                : initial event number
    --store-fail <True|False>       : store output files for failed jobs in chain processing.
    --read-dbs                      : DBS URL used for obtaining the list of available blocks for real data. Default: http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet
    --scripts-dir                   : Path to workflow creation scripts (default: $PUTIL)
    --skip-config                   : Is the configuration file was already created, it will skip cmsDriver command execution
    --extra-label                   : Extra label for identifying the datasets: /RelVal*/CMSSW_X_Y_Z-<Conditions>_<SpecialTag>_<ExtraLabel>_<FilterName>-<version>/TIER
    --help (-h)                     : help
    --debug (-d)                    : debug statements
    
    
    """
    
    # default
    try:
        version = os.environ.get("CMSSW_VERSION")
    except:
        print ''
        print 'CMSSW version cannot be determined from $CMSSW_VERSION'
        sys.exit(2)

    try:
        architecture = os.environ.get("SCRAM_ARCH")
    except:
        print ''
        print 'CMSSW architecture cannot be determined from $SCRAM_ARCH'
        sys.exit(2)

    try:
        from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
    except ImportError, ex:
        print ex
        print 'Please load prodAgent libraries (point $PYTHONPATH to the right path).'
        sys.exit(2)

    samplesFile = None
    processing_version = None
    initial_run = "666666"
    initial_event = None
    debug = False
    DBSURL = None
    pileup_dataset = None
    storeFail = False
    readDBS = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
    onlySites = None
    scriptsDir = '$PUTIL'
    skip_config = False
    extra_label = ''

    try:
        opts, args = getopt.getopt(argv, "", ["help", "debug", "samples=", "version=", 
                                                "DBSURL=", "event=", "lumi=", "pileupdataset=", 
                                                "store-fail=", "read-dbs=", "only-sites=", 
                                                "scripts-dir=", "skip-config", "extra-label="])
    except getopt.GetoptError:
        print main.__doc__
        sys.exit(2)

    # check command line parameter
    for opt, arg in opts :
        if opt == "--help" :
            print main.__doc__
            sys.exit()
        elif opt == "--debug" :
            debug = True
        elif opt == "--samples" :
            samplesFile = arg
        elif opt == "--version" :
            processing_version = arg
        elif opt == "--lumi" :
            initial_run = arg
        elif opt == "--event" :
            initial_event = arg
    	elif opt == "--DBSURL" :
            DBSURL = arg
        elif opt == "--pileupdataset" :
            pileup_dataset = arg
            print arg
        elif opt == '--store-fail':
            if arg.lower() in ("true", "yes"):
                storeFail = True
            else:
                storeFail = False
        elif opt == '--read-dbs':
            readDBS = arg
        elif opt == '--only-sites':
            onlySites = arg
        elif opt == '--scripts-dir':
            if arg.endswith('/') :
                scriptsDir = arg[:-1]
            else:
                scriptsDir = arg
            scriptsDirTemp = scriptsDir
            if scriptsDir.startswith('$') :
                scriptsDirTemp = os.environ.get(scriptsDir[1:],None)
            if scriptsDirTemp != None:
                if not os.path.exists(scriptsDirTemp):
                    print "--scripts-dir argument does not exist, please verify."
                    sys.exit(6)
            else:
                print "--scripts-dir argument does not exist, please verify."
                sys.exit(6)
        elif opt == "--skip-config":
            skip_config = True
        elif opt == "--extra-label":
            extra_label = arg

    if initial_event == None :
	print ""
	print "Warning: Initial Event Number is not set, output workflow will not have this block."
	print ""

    if samplesFile == None or processing_version == None or DBSURL == None :
        print main.__doc__
        sys.exit(2)
    
    samples = []
    steps = {}
    primary_prefix = 'RelVal'    
    max_step = 1

    try:
        file = open(samplesFile)
    except IOError:
        print 'file with list of parameter-sets cannot be opened!'
        sys.exit(1)
    n_line = 0
    print 'Parsing input file...'
    for line in file.readlines():
        n_line += 1
        # Skipping lines with no info
        if line.strip() != '' and line.strip() != '\n' and \
            not line.strip().startswith("#") and \
            line.find('//') != 0: # I don't know what's the last condition for
            line_parts = [part.strip() for part in line.split('@@@') if part]
            #  //
            # // Parsing first step
            #//
            if not line.strip().startswith('STEP'):
                command = ''
                array = []
                special_tag = ''
                conditions = None
                total_events = None
                events_per_job = None
                pile_up = False
                output_name = ''
                input_data = {}
                input_blocks = ""
                
                sample_info = line_parts[0].strip()
                #  //
                # // Filling up sample's details
                #//
                sample_info_parts = [part.strip() for part in \
                                    sample_info.split('++') if part]
                sample_number = sample_info_parts[0] #We might need this later
                sample_name = sample_info_parts[1]
                sample_steps = [i.strip() for i in \
                                    sample_info_parts[2].split(',') if i]
                primary = primary_prefix + sample_name
                #  //
                # // Is it a real data processing sample? According to this 
                #// we assign or not the command variable.
                #\\
                if line_parts[0].find('REALDATA') > -1:
                    is_real_data = True
                else:
                    is_real_data = False
                    command = line_parts[1].strip()
                    #  //
                    # // Clean cmsDriver command format
                    #//
                    if command.find('=') > -1:
                        command = command.replace('=',' ')
                    array = [i for i in command.split() if i]
                    #  //
                    # // Remove --python_filename if present
                    #//
                    if '--python_filename' in array:
                        del array[array.index('--python_filename'):\
                            array.index('--python_filename')+2]
                    command = " ".join(array)
                    #  //
                    # // Parse conditions
                    #//
                    if '--conditions' in array:
                        conditions = array[array.index('--conditions')+1\
                            ].split(',')[1].split('::')[0].strip()
                    else:
                        conditions = 'SpecialConditions'
                    #  //
                    # // Parsing number of events
                    #//
                    if '--relval' in array :
                        total_events = array[array.index('--relval')+1\
                            ].split(',')[0].strip()
                        events_per_job = array[array.index('--relval')+1\
                            ].split(',')[1].strip()
                    #  //
                    # // Special tag
                    #//
                    # FastSim
                    if command.find('FASTSIM') > -1:
                        special_tag = 'FastSim'
                    # PileUp (at the same time with FastSim)
                    if '--pileup' in array :
                        #  //
                        # // Will use whatever argument of --pileup option is
                        #//
                        pileup_arg = array[array.index('--pileup') + 1]
                        if pileup_arg.lower().strip() != 'nopileup':
                            if special_tag:
                                special_tag = "_".join(
                                    [special_tag, pileup_arg.strip()])
                            else:
                                special_tag = pileup_arg.strip()
                            pile_up = True
                            if pileup_dataset is None :
                                print "You have to provide a pileup dataset."
                                print "Usually it is a MinBias (RAW)."
                                print "Use option --pileupdataset"
                                sys.exit(5)
                    #  //
                    # // Sort of custom tag
                    #//
                    if '--beamspot' in array:
                        beamspot_arg = \
                            array[array.index('--beamspot') + 1].strip()
                        if special_tag:
                            special_tag = "_".join(
                                [special_tag, beamspot_arg])
                        else:
                            special_tag = beamspot_arg
                    #  //
                    # // Cfg file's output name
                    #//
                    output_name = "_".join(
                        [x for x in [primary, conditions, special_tag] if x]
                        ) + ".py"
                    #  //
                    # // Add command options
                    #//
                    if command.find('no_exec') < 0:
                        command += ' --no_exec'
                    if command.find('python_filename') < 0:
                        command += ' --python_filename ' + output_name
                #  //
                # // Collecting info for real data samples
                #//
                if is_real_data:
                    #  //
                    # // Parsing dataset details. The following details are
                    #// supported: REALDATA, RUN, LABEL, FILES, EVENTS.
                    #\\
                    for parameter in sample_info_parts[3].split(','):
                        input_data[parameter.split(':')[0].strip()] = \
                            parameter.split(':')[1].strip()
                    #  //
                    # // Verifiying optional arguments: LABEL, FILE, EVENTS
                    #//
                    data_label = input_data.get('LABEL', '')
                    data_files = input_data.get('FILES', '')
                    data_events = input_data.get('EVENTS', '')
                    if data_events:
                        data_events = int(data_events)
                    if data_files:
                        data_files = int(data_events)
                    #  //
                    # // Extra tag: RelVal string should be in the processed
                    #// dataset name. I will use the primary_prefix for now.
                    #\\
                    if data_label:
                        special_tag = "_".join([primary_prefix, data_label])
                    else:
                        special_tag = primary_prefix
                    #  //
                    # // Looking up the blocks for a given Dataset and a given run
                    #//
                    reader = DBSReader(readDBS)
                    input_files = reader.dbs.listFiles(path=input_data['REALDATA'], \
                        runNumber=input_data['RUN'])
                    blocks = {}
                    #  //
                    # // Parsing input blocks
                    #//
                    for input_file in input_files:
                        cur_files = \
                            blocks.setdefault(input_file['Block']['Name'],
                                              {}).setdefault('Files', 0)
                        cur_events = \
                            blocks[input_file['Block']['Name']].setdefault(
                                'Events', 0)
                        blocks[input_file['Block']['Name']]['Files'] += 1
                        blocks[input_file['Block']['Name']]['Events'] += \
                            input_file['NumberOfEvents']
                    #  //
                    # // Truncating blocks list
                    #//
                    total_events = 0
                    total_files = 0
                    blocks_to_process = []
                    for block in blocks:
                        blocks_to_process.append(block)
                        total_events += blocks[block]['Events']
                        total_files += blocks[block]['Files']
                        if data_events and (data_events < total_events):
                            break
                        if data_files and (data_files < total_files):
                            break

                    input_blocks = ",".join(blocks_to_process)
                #  //
                # // Composing a dictionary per sample
                #//
                dict = {}
                dict['sampleName'] = sample_name
                dict['command'] = command
                dict['primary'] = primary
                dict['outputName'] = output_name
                dict['conditions'] = conditions
                dict['specialTag'] = special_tag 
                dict['totalEvents'] = total_events
                dict['eventsPerJob'] = events_per_job
                dict['pileUp'] = pile_up
                dict['isRealData'] = is_real_data
                dict['inputData'] = input_data
                dict['inputBlocks'] = input_blocks
                dict['steps'] = sample_steps
 
                samples.append(dict)

                if debug:
                    print 'Parsing'
                    print 'Sample:', sample_name
                    print 'Command:', command
                    print 'Conditions:', conditions
                    print 'Special tag:', special_tag
                    print 'Total events:', total_events
                    print 'Events per job:', events_per_job
                    print 'Steps:', sample_steps
                    print 'PileUp:', pile_up
                    print 'Input data:', input_data
                    print 'Input blocks', input_blocks
                    print ''

            #  //
            # // No a first step command (secon HLT table, RECO, ALCA, etc)
            #//
            else:
                step_number = int(line_parts[0].split('++')[0].strip()[-1])
                step_name = line_parts[0].split('++')[1].strip()
                command = line_parts[1].strip()
                #  //
                # // Clean cmsDriver command format
                #//
                if command.find('=') > -1:
                    command = command.replace('=',' ')
                array = [i for i in command.split() if i]
                #  //
                # // Remove --python_filename if present
                #//
                if '--python_filename' in array:
                    del array[array.index('--python_filename'):\
                        array.index('--python_filename')+2]
                command = " ".join(array)
                #  //
                # // Parse conditions
                #//
                if '--conditions' in array:
                    conditions = array[array.index('--conditions')+1\
                        ].split(',')[1].split('::')[0].strip()
                else:
                    conditions = 'SpecialConditions'
                #  //
                # // Cfg file's output name
                #//
                output_name = "_".join([step_name, conditions]) + ".py"
                #  //
                # // Add command options
                #//
                if command.find('no_exec') < 0:
                    command += ' --no_exec'
                if command.find('python_filename') < 0:
                    command += ' --python_filename ' + output_name
                #  //
                # // Second trigger table? This may be changed, right now I am
                #// assuming that all 4 steps workflows are like this.
                #\\
                stage_previous = True
                if step_number == 2:
                    if '-s' in array:
                        index = array.index('-s')
                    else:
                        index = array.index('--step')
                    if array[index+1].find('RECO') < 0:
                        stage_previous = False

                if step_number > max_step:
                    max_step = step_number
                #  //
                # // Composing a dictionary per step
                #//
                dict = {}
                dict['stepNumber'] = step_number
                dict['command'] = command
                dict['outputName'] = output_name
                dict['conditions'] = conditions
                dict['stagePrevious'] = stage_previous
                #  //
                # // Step name should be unique
                #//
                if step_name not in steps:
                    steps[step_name] = dict
                else:
                    print "Label %s is repeated!!!" % step_name
                    sys.exit(1)

                if debug:
                    print 'Parsing'
                    print 'Step name:', step_name
                    print 'Step number:', step_number
                    print 'Command:', command
                    print 'Conditions:', conditions
                    print 'Stage previous:', stage_previous
                    print ''

    file.close()

    if debug:
        print "Collected information step 1"
        for sample in samples:
            print 'Sample name:', sample['sampleName']
            print 'Command', sample['command']
            print 'Real data:', sample['isRealData']
            print 'Input data:', sample['inputData']
            print 'Input blocks', sample['inputBlocks']
            print 'Conditions:', sample['conditions']
            print 'Total events:', sample['totalEvents']
            print 'Events per job:', sample['eventsPerJob']
            print 'Output name:', sample['outputName']
            print 'Steps:', sample['steps']
            print 'PileUp:', sample['pileUp']
            print 'Special tag:', sample['specialTag']
            print ''
        for i in range(2, max_step+1):
            print 'Collected information step %s' % i
            for step in steps:
                if steps[step]['stepNumber'] == i:
                    print 'Step name:', step
                    print 'Command:', steps[step]['command']
                    print 'Conditions:', steps[step]['conditions']
                    print 'Stage previous:', steps[step]['stagePrevious']
                    print ''

    #  //
    # // Execute cmsDriver command
    #//
    print ''
    print 'Executing cmsDriver commands for step 1 configurations'
    print ''
    for sample in samples:
        if not sample['isRealData']:
            #  //
            # // if the cfg. file was already created, we'll skip cmsDriver
            #// command execution.
            #\\
            if os.path.exists("/".join([os.getcwd(),
                sample['outputName']])) and skip_config:
                print 'cmsDriver command for step 1 to produce:', \
                    sample['outputName'],'was already issued, skipping.'
                continue
            proc = popen2.Popen3(sample['command'])
            exitCode = proc.wait()
            if exitCode == 0:
                print 'cmsDriver command for step 1 to produce:', \
                    sample['outputName'],'exited with ExitCode:', exitCode
            else :
                print 'cmsDriver command for step 1 to produce:', \
                    sample['outputName'],'failed with ExitCode:', exitCode
                sys.exit(1)
        else :
            msg = 'Real Data:\n'
            msg += 'Input dataset: %s\n' % (sample['inputData']['REALDATA'])
            msg += 'Run: %s\n' % (sample['inputData']['RUN'])
            msg += 'Input blocks: %s' % (sample['inputBlocks'])
            print msg

    for i in range(2, max_step+1):
        print ''
        print 'Executing cmsDriver commands for step %s configurations' % i
        print ''
        for step in steps:
            if steps[step]['stepNumber'] == i:
                #  //
                # // if the cfg. file was already created, we'll skip cmsDriver
                #// command execution.
                #\\
                if os.path.exists("/".join([os.getcwd(),
                    steps[step]['outputName']])) and skip_config:
                    print 'cmsDriver command for step %s to produce:' % i, \
                        steps[step]['outputName'],'was already issued, skipping.'
                    continue
                proc = popen2.Popen3(steps[step]['command'])
                exitCode = proc.wait()
                if exitCode == 0:
                    print 'cmsDriver command for step %s to produce:' % i, \
                        steps[step]['outputName'], \
                        'exited with ExitCode:', exitCode
                else:
                    print 'cmsDriver command for step %s to produce:' % i, \
                        steps[step]['outputName'], \
                        'failed with ExitCode:', exitCode
                    sys.exit(1)

    print ''
    print 'Workflow creation'
    print ''

    datasets = []
    unmergedDatasets = []
    mergedDatasets = []
    workflows = {}
    
    #  //
    # // Create workflows
    #//
    for sample in samples:
        command = 'python2.4 ' + scriptsDir
        conditions = '' # Conditions -> processingString
        #  //
        # // In case we are processing data
        #//
        if sample['isRealData']:
            command += '/createProcessingWorkflow.py \\\n'
            # Not changing the primary dataset name for real data.
            #command += '--override-channel=' + sample['primary'] + ' \\\n'
            command += '--dataset=' + sample['inputData']['REALDATA'] + ' \\\n'
            command += '--only-blocks=' + sample['inputBlocks'] + ' \\\n'
            command += '--dbs-url=' + readDBS + ' \\\n'
            conditions = steps[sample['steps'][0]]['conditions']
            command += '--split-type=file \\\n'
            command += '--split-size=1 \\\n'
        #  //
        # // MC workflows
        #//
        else:
            command += '/createProductionWorkflow.py \\\n'
            command += '--channel=' + sample['primary'] + ' \\\n'
            conditions = sample['conditions']
            command += '--starting-run=' + initial_run + ' \\\n'
            if initial_event != None:
                command += '--starting-event=' + initial_event + ' \\\n'
            command += '--totalevents=' + sample['totalEvents'] + ' \\\n'
            command += '--eventsperjob=' + sample['eventsPerJob'] + ' \\\n'
            if sample['pileUp']:
                command += '--pileup-dataset=' + pileup_dataset + ' \\\n'
            if storeFail:
                command += '--store-fail=True \\\n'
            #  //
            # // First step
            #//
            command += '--version=' + version + ' \\\n'
            command += '--py-cfg=' + sample['outputName'] + ' \\\n'
        #  //
        # // Input configurations (Second step and further)
        #//
        if sample['steps'][0].lower().strip() != 'none':
            for i, step in enumerate(sample['steps']):
                command += '--version=' + version + ' \\\n'
                command += '--py-cfg=' + steps[step]['outputName'] + ' \\\n'
                if i != 0 or not sample['isRealData']:
                    command += '--stageout-intermediates=%s \\\n' % (
                        steps[step]['stagePrevious'])
                    command += '--chained-input=output \\\n'
                #  //
                # // If a two-hlt tables workflow, will take conditions from
                #// the second step information
                #\\
                if not steps[step]['stagePrevious'] and \
                    i == 0:
                    conditions = steps[step]['conditions']
        #  //
        # // Common options
        #//
        command += '--group=RelVal \\\n'
        command += '--category=relval \\\n'
        command += '--activity=RelVal \\\n'
        command += '--acquisition_era=' + version + ' \\\n'
        command += '--only-sites=' + onlySites + ' \\\n'
        command += '--processing_version=' + processing_version + ' \\\n'
        #  //
        # // processingString="Conditions"_"specialTag"_"extra-label"
        #//
        processing_string = conditions
        if sample['specialTag']:
            processing_string = "_".join([processing_string, 
                sample['specialTag']])
        if extra_label:
            processing_string = "_".join([processing_string, 
                extra_label])
        command += '--processing_string=' + processing_string

        if debug:
            print command
            print ''
        
        proc = popen2.Popen3(command)
        exitCode = proc.wait()
        output = proc.fromchild.readlines()

        if debug:
            print output
            print ''

        if exitCode == 0:
            #parse output
            tmp = []
            index = FindIndex(output,'Output Datasets')
            for dataset in output[index+1:] : tmp.append(dataset.strip())
            datasets.append({'unmerged': tmp,
                            'totalEvents': sample['totalEvents'],
                            'merged': [x.replace('-unmerged','') for x in tmp]
                            })
            unmergedDatasets.append(tmp)
            index = FindIndex(output,'Created')
            if index == -1:
                print "No workflow was created by create*workflow.py"
                sys.exit(1)
            workflow = output[index].split()[1].strip()
            workflows[workflow] = sample['isRealData']
            print 'workflow creation command for workflow:', workflow, \
                'exited with ExitCode:', exitCode
        else :
            print 'workflow creation command:'
            print command
            print 'failed'
            sys.exit(1)

    if debug:
        print 'Created workflows:'
        print workflows.keys()	
        print ''
        print "Unmerged datasets:"
        print unmergedDatasets

    # extract merged datasets
    for sample in unmergedDatasets:
        tmp = []
        for dataset in sample:
            tmp.append(dataset.replace('-unmerged',''))
        mergedDatasets.append(tmp)

    print ''
    print 'Write helper scripts'
    print ''

    # WorkflowInjector:Input script
    inputScript = open('input.sh','w')
    inputScript.write('#!/bin/bash\n')
    feeder = 'None'
    for workflow in workflows.keys():
        if workflows[workflow]:
            if feeder.find('ReReco') < 0:
                inputScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py WorkflowInjector:SetPlugin BlockFeeder\n')
                feeder = 'ReReco'
        else :
            if feeder.find('Request') < 0:
                inputScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py WorkflowInjector:SetPlugin RequestFeeder\n')
                feeder = 'Request'
        inputScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py WorkflowInjector:Input ' + os.path.join(os.getcwd(), workflow) + '\n')
    inputScript.close()
    os.chmod('input.sh',0755)
    print 'Wrote WorkflowInjector:Input script to:',os.path.join(os.getcwd(),'input.sh') 

    # ForceMerge
    forceMergeScript = open('forceMerge.sh','w')
    forceMergeScript.write('#!/bin/bash\n')
    for sample in unmergedDatasets :
        for dataset in sample :
            forceMergeScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py ForceMerge ' + dataset + '\n')
    forceMergeScript.close()
    os.chmod('forceMerge.sh',0755)
    print 'Wrote ForceMerge script to:',os.path.join(os.getcwd(),'forceMerge.sh')

    # MigrateDatasetToGlobal
    migrateScript = open('migrateToGlobal.sh','w')
    migrateScript.write('#!/bin/bash\n')
    for sample in mergedDatasets :
        for dataset in sample :
            migrateScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py DBSInterface:MigrateDatasetToGlobal ' + dataset + '\n')
    migrateScript.close()
    os.chmod('migrateToGlobal.sh',0755)
    print 'Wrote DBSInterface:MigrateDatasetToGlobal script to:',os.path.join(os.getcwd(),'migrateToGlobal.sh')

    # PhEDExInjectDataset
    phedexScript = open('injectIntoPhEDEx.sh','w')
    phedexScript.write('#!/bin/bash\n')
    for sample in mergedDatasets :
        for dataset in sample :
            phedexScript.write('python2.4 $PRODAGENT_ROOT/util/publish.py PhEDExInjectDataset ' + dataset + '\n')
    phedexScript.close()
    os.chmod('injectIntoPhEDEx.sh',0755)
    print 'Wrote PhEDExInjectDataset script to:',os.path.join(os.getcwd(),'injectIntoPhEDEx.sh')

    # DBS: query unmerged datasets
    queryUnmergedScript = open('queryUnmerged.sh','w')
    queryUnmergedScript.write('#!/bin/bash\n')
    for sample in unmergedDatasets :
        for dataset in sample :
            #if dataset.find('-RECO') == -1 or len(sample) == 1 :
            queryUnmergedScript.write('python2.4 $PRODAGENT_ROOT/util/InspectDBS2.py --DBSURL=' + DBSURL  + ' --datasetPath=' + dataset + ' | grep total\n')
    queryUnmergedScript.close()
    os.chmod('queryUnmerged.sh',0755)
    print 'Wrote DBS query script for unmerged datasets to:',os.path.join(os.getcwd(),'queryUnmerged.sh')

    # DBS: query merged datasets
    queryMergedScript = open('queryMerged.sh','w')
    queryMergedScript.write('#!/bin/bash\n')
    for sample in mergedDatasets :
        for dataset in sample :
            #if dataset.find('-RECO') == -1 or len(sample) == 1 :
            queryMergedScript.write('python2.4 $PRODAGENT_ROOT/util/InspectDBS2.py --DBSURL=' + DBSURL  + ' --datasetPath=' + dataset + ' | grep total\n')
    queryMergedScript.close()
    os.chmod('queryMerged.sh',0755)
    print 'Wrote DBS query script for merged datasets to:',os.path.join(os.getcwd(),'queryMerged.sh')

    # DQMHarvesting
    DQMinputScript = open('DQMinput.sh','w')
    DQMinputScript.write("#!/bin/bash\n")
    for sample in mergedDatasets :
        for dataset in sample :
            if dataset.find('RECO') != -1 :
                primary = dataset.split("/")[1]
                processed = dataset.split("/")[2]
                tier = dataset.split("/")[3]
                DQMinputScript.write('python2.4 $PRODAGENT_ROOT/util/harvestDQM.py  --run=1 --primary=' + primary  + ' --processed=' + processed + ' --tier=' + tier + '\n' )
    os.chmod('DQMinput.sh',0755)
    print 'Wrote DQMHarvesting script for merged datasets to:', os.path.join(os.getcwd(),'DQMinput.sh')

    # Output datasets list
    outputList = open('outputDatasets.txt','w')
    for sample in mergedDatasets :
        for dataset in sample :
            outputList.write(dataset + "\n")
    print 'Wrote output datasets list to:', os.path.join(os.getcwd(),'outputDatasets.txt')

    # File with expected number of events
    numberOfEvents = open('eventsExpected.txt','w')
    for sample in datasets:
        for dataset in sample['merged']:
            numberOfEvents.write("%s %s\n" % (sample['totalEvents'],dataset))
    numberOfEvents.close()
    print 'Wrote events per dataset to:', os.path.join(os.getcwd(),'eventsExpected.txt')

    print ''

def FindIndex(output,string) :
    index = -1
    counter = 0
    for field in output :
        if field.find(string) != -1 : index = counter
        counter += 1
    return index
 
if __name__ == '__main__' :
    main(sys.argv[1:])
