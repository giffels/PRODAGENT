#!/usr/bin/env python

import sys
import os
import getopt
import subprocess
import shlex
import re
import xml.sax, xml.sax.handler
from xml.sax.saxutils import escape
import time

from Configuration.PyReleaseValidation.autoCond import autoCond

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
    --DBSURL <URL>                  : URL of the local DBS (http://cmsdbsprod.cern.ch/cms_dbs_prod_local_07/servlet/DBSServlet | http://cmssrv46.fnal.gov:8080/DBS208/servlet/DBSServlet)
    --only-sites                    : Site where dataset is going to be processed or where the input dataset is taken from. Usually srm-cms.cern.ch and cmssrm.fnal.gov
    
    optional parameters
    --pileupdataset                 : input pileup dataset. It must be provided if the <samples> txt file contains PilepUp samples
    --lumi <number>                 : initial run for generation (default: 666666), set it to 777777 for high statistics samples
    --event <number>                : initial event number (default: 1)
    --store-fail                    : store output files for failed jobs in chain processing.
    --read-dbs                      : DBS URL used for obtaining the list of available blocks for real data. Default: http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet
    --scripts-dir                   : Path to workflow creation scripts (default: $PUTIL)
    --skip-config                   : Is the configuration file was already created, it will skip cmsDriver command execution
    --extra-label                   : Extra label for identifying the datasets: /RelVal*/CMSSW_X_Y_Z-<Conditions>_<SpecialTag>_<ExtraLabel>_<FilterName>-<version>/TIER
    --workflow-label                : Label for the workflows.
    --help (-h)                     : help
    --debug (-d)                    : debug statements
    
    
    """
    
    start_total_time = time.time()

    # default
    version = os.environ.get("CMSSW_VERSION")
    if version is None:
        print ''
        print 'CMSSW version cannot be determined from $CMSSW_VERSION'
        sys.exit(2)

    architecture = os.environ.get("SCRAM_ARCH")
    if architecture is None:
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
    initial_event = "1"
    debug = False
    DBSURL = None
    pileup_dataset = None
    storeFail = False
    readDBS = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
    onlySites = None
    scriptsDir = '$PUTIL' #os.path.expandvars(os.environ.get('PUTIL', None))
    skip_config = False
    extra_label = ''
    workflow_label = ''

    try:
        opts, args = getopt.getopt(argv, "", ["help", "debug", "samples=", "version=", 
                                                "DBSURL=", "event=", "lumi=", "pileupdataset=", 
                                                "store-fail", "read-dbs=", "only-sites=", 
                                                "scripts-dir=", "skip-config", "extra-label=",
                                                "workflow-label="])
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
            storeFail = True
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
            # There's no need to expand the shell variables anymore
            #if scriptsDir.startswith('$') :
            #    scriptsDirTemp = os.environ.get(scriptsDir[1:],None)
            #    scriptsDir = os.path.expandvars(scriptsDirTemp)
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
        elif opt == "--workflow-label":
            workflow_label = arg

    if samplesFile == None or processing_version == None or DBSURL == None :
        print main.__doc__
        sys.exit(2)

    if debug:
        print "\nprepareRelValWorkflows.py was started with the following arguments: %s" % \
            " ".join(argv)
        print "\n"
 
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
    start_parse_time = time.time()
    for line in file.readlines():
        n_line += 1
        # Skipping lines with no info
        if line.strip() != '' and line.strip() != '\n' and \
            not line.strip().startswith("#") and \
            line.find('//') != 0: # I don't know what's the last condition for
            line_parts = [part.strip() for part in line.split('@@@') if part]
            dqmData = {} # Keys: Scenario, Run
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
                acq_era = version
                
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
                    #  //
                    # // Parse conditions
                    #//
                    if '--conditions' in array:
                        conditions_arg = array[array.index('--conditions')+1]
                        if conditions_arg.startswith('auto:'):
                            conditions_key = conditions_arg.split('auto:')[1]
                            conditions_value = autoCond[conditions_key]
                        else:
                            conditions_value = conditions_arg
                        conditions = [
                            x.strip() \
                            for x in conditions_value.split(',') \
                            if x.find("::") != -1
                            ][0].split('::')[0].strip()
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
                                    [special_tag, "PU", pileup_arg.strip()])
                            else:
                                special_tag = "_".join(["PU", 
                                                        pileup_arg.strip()])
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
                        array.append('--no_exec')
                    if command.find('python_filename') < 0:
                        array.append('--python_filename')
                        array.append(output_name)
                    # Recomposing cmsDriver command
                    command = " ".join(array)

                    # Filling up DQM information
                    dqmData['Runs'] = '1'
                    dqmData['Scenario'] = getDQMScenario(command)

                #  //
                # // Collecting info for real data samples
                #//
                if is_real_data:
                    #  //
                    # // Parsing dataset details. The following details are
                    #// supported: REALDATA, RUN, LABEL, FILES, EVENTS, PDNAME
                    #\\
                    # Producing tuples from the input options.
                    data_options = [tuple(x.split(':')) \
                        for x in sample_info_parts[3].split(',') if x.strip()]
                    # Parsing tuples
                    for arg_v in data_options:
                        if len(arg_v) == 2:
                            input_data[arg_v[0].strip()] = arg_v[1].strip()
                        elif len(arg_v) == 1:
                            input_data[arg_v[0].strip()] = None
                        else:
                            print "Line %s has an extra ','." % (line)
                            sys.exit(7)
                    #  //
                    # // Verifiying optional arguments: RUN, LABEL, FILE, EVENTS,
                    #// PRIMARY
                    #\\
                    data_run = input_data.get('RUN', '')
                    data_label = input_data.get('LABEL', '')
                    data_files = input_data.get('FILES', '')
                    data_events = input_data.get('EVENTS', '')
                    data_pname = input_data.get('PRIMARY', None)
                    if data_events:
                        data_events = int(data_events)
                    if data_files:
                        data_files = int(data_events)
                    #  //
                    # // Looking for best matching dataset. It should be just
                    #// one, otherwise the script will exit.
                    #\\
                    reader = DBSReader(readDBS)
                    query = "find dataset where dataset like %s" % (
                        input_data['REALDATA'])
                    result_xml = reader.dbs.executeQuery(query)
                    # XML Handler
                    result_list = DBSXMLParser(result_xml)
                    target_datasets = [x['dataset'] for x in result_list]

                    # If more than one dataset is found.
                    if len(target_datasets) > 1:
                        # Is this an input relval dataset produced in the
                        # current release?
                        query = "find dataset where dataset like %s " % (
                            input_data['REALDATA'])
                        query += "and release=%s" % version
                        result_xml = reader.dbs.executeQuery(query)
                        result_list = DBSXMLParser(result_xml)
                        target_datasets = [x['dataset'] for x in result_list]

                    # If more than one dataset is found, match the processing
                    # version
                    if len(target_datasets) > 1:
                        find_version = \
                            lambda x: x.find(processing_version) != -1
                        target_datasets = filter(find_version, target_datasets)

                    if len(target_datasets) > 1:
                        msg = "Dataset pattern in line %s is too broad." % line
                        msg += "These datasets were found: %s" % (
                            " ".join(target_datasets))
                        print msg
                        sys.exit(8)

                    if not target_datasets:
                        msg = "Dataset pattern produced no match in line %s" % (
                            line)
                        print msg
                        sys.exit(8)
                    # Now I can look up the blocks for this dataset.
                    target_dataset = target_datasets[0]
                    input_data['REALDATA'] = target_dataset
                    #  //
                    # // Looking up the blocks for a given Dataset and the
                    #// provided list of runs
                    #\\
                    runs_list = \
                        [x.strip() for x in data_run.split('|') if x.strip()]
                    runs_in_dbs = [x['RunNumber'] for x in \
                                        reader.dbs.listRuns(target_dataset)]
                    runs_in_dbs.sort()
                    # Creating lambda function for filtering runs.
                    # Do filtering only if a run list was requested
                    if runs_list:
                        expr = ''
                        # First a string expression to evaluate
                        is_the_first = True
                        for run in runs_list:
                            if is_the_first:
                                expr += "("
                                is_the_first = False
                            else:
                                expr += " or "
                            # Run range: XXXXXX-XXXXXX
                            if run.count("-"):
                                run_limits = \
                                    [x.strip() for x in run.split('-') if x.strip()]
                                expr += "(x >= %s and x <= %s)" % (
                                                    run_limits[0], run_limits[1])
                            else:
                                expr += "x == %s" % run
                        if not is_the_first:
                            expr += ")"
                        # Here comes the lambda funtion
                        runs_filter = lambda x: eval(expr)
                        # Filtering runs in DBS using the list provided in the
                        # input file.
                        target_runs = filter(runs_filter, runs_in_dbs)
                    else:
                        target_runs = runs_in_dbs

                    # Pulling up input files from DBS (including run info).
                    input_files = reader.dbs.listFiles(
                                                path=target_dataset,
                                                retriveList=['retrive_run'])
                    #  //
                    # // Parsing input blocks
                    #//
                    blocks = {}
                    for input_file in input_files:
                        # Skip files with no events
                        # A block will be skipped if all its files have 0
                        # events
                        if input_file['NumberOfEvents'] == 0:
                            continue
                        runs = \
                            [int(x['RunNumber']) for x in input_file['RunsList']]
                        for run in runs:
                            if run in target_runs:
                                break
                        else:
                            continue # skip file if it's not in the target_runs
                        cur_files = \
                            blocks.setdefault(input_file['Block']['Name'],
                                              {}).setdefault('Files', 0)
                        cur_events = \
                            blocks[input_file['Block']['Name']].setdefault(
                                'Events', 0)
                        cur_runs = \
                            blocks[input_file['Block']['Name']].setdefault(
                                'Runs', set())
                        blocks[input_file['Block']['Name']]['Files'] += 1
                        blocks[input_file['Block']['Name']]['Events'] += \
                                                    input_file['NumberOfEvents']
                        blocks[input_file['Block']['Name']]['Runs'] = \
                            cur_runs.union(runs)

                    #  //
                    # // Truncating blocks list
                    #//
                    total_events = 0
                    total_files = 0
                    blocks_to_process = []
                    runs_to_process = set()
                    for block in blocks:
                        blocks_to_process.append(block)
                        runs_to_process = runs_to_process.union(blocks[block]['Runs'])
                        total_events += blocks[block]['Events']
                        total_files += blocks[block]['Files']
                        if data_events and (data_events < total_events):
                            break
                        if data_files and (data_files < total_files):
                            break

                    input_blocks = ",".join(blocks_to_process)

                    #  //
                    # // If PRIMARY is true, then it will use the 
                    #// sample_name value as primary dataset name, else it 
                    #\\ will use the input primary dataset name.
                    # \\
                    if data_pname is not None and \
                            data_pname.lower() in ('y', 't', 'true'):
                        primary = "".join([primary_prefix, sample_name])
                    else:
                        primary = \
                         [x for x in input_data['REALDATA'].split("/") if x][0]

                    #   //
                    #  // Seting special tag
                    #//
                    special_tag_parts = []
                    # Add RelVal tag if not present.
                    if target_dataset.find(primary_prefix) == -1:
                        special_tag_parts.append(primary_prefix)
                    # Add LABEL
                    if data_label:
                        special_tag_parts.append(data_label)
                    special_tag = "_".join(special_tag_parts)

                    #  //
                    # // Setting Acq. Era
                    #//
                    #processed_dataset = target_dataset.split('/')[2]
                    #dataset_acq_era = processed_dataset.split("-")[0]
                    #if dataset_acq_era.startswith(version):
                    #    acq_era = version
                    #else:
                    #    acq_era = dataset_acq_era

                    # Filling up DQM information
                    dqmData['Runs'] = \
                        ",".join([str(x) for x in list(runs_to_process)])

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
                dict['AcqEra'] = acq_era
                dict['DQMData'] = dqmData
 
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
                    print 'Input blocks:', input_blocks
                    print 'DQMData:', dqmData
                    print ''

            #  //
            # // No a first step command (second HLT table, RECO, ALCA, etc)
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
                #  //
                # // Parse conditions
                #//
                if '--conditions' in array:
                    conditions_arg = array[array.index('--conditions')+1]
                    if conditions_arg.startswith('auto:'):
                        conditions_key = conditions_arg.split('auto:')[1]
                        conditions_value = autoCond[conditions_key]
                    else:
                        conditions_value = conditions_arg
                    conditions = [
                        x.strip() \
                        for x in conditions_value.split(',') \
                        if x.find("::") != -1
                        ][0].split('::')[0].strip()
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
                    array.append('--no_exec')
                if command.find('python_filename') < 0:
                    array.append('--python_filename')
                    array.append(output_name)
                # Recomposing cmsDriver command
                command = " ".join(array)
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
                # // HARVESTING cmsDriver commands should be ignored. RelVals
                #// should not run any HARVESTING configuration. Harvestings
                #\\ run independently after the datasets are produced.
                # \\
                skip_step = False
                if '-s' in array:
                    index = array.index('-s')
                else:
                    index = array.index('--step')
                if array[index+1].count('HARVESTING') > 0:
                    skip_step = True

                #  //
                # // Composing a dictionary per step
                #//
                dict = {}
                dict['stepNumber'] = step_number
                dict['command'] = command
                dict['outputName'] = output_name
                dict['conditions'] = conditions
                dict['stagePrevious'] = stage_previous
                dict['DQMData'] = {'Scenario': getDQMScenario(command)}
                dict['skipStep'] = skip_step
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
                    print 'DQM Data:', dict['DQMData']
                    print ''

    parse_time = time.time() - start_parse_time
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
            print 'Acq. Era:', sample['AcqEra']
            print 'DQM data:', sample['DQMData']
            print ''
        for i in range(2, max_step+1):
            print 'Collected information step %s' % i
            for step in steps:
                if steps[step]['stepNumber'] == i:
                    print 'Step name:', step
                    print 'Command:', steps[step]['command']
                    print 'Conditions:', steps[step]['conditions']
                    print 'Stage previous:', steps[step]['stagePrevious']
                    print 'DQM Data:', steps[step]['DQMData']
                    print ''

    #  //
    # // Execute cmsDriver command
    #//
    print ''
    print 'Executing cmsDriver commands for step 1 configurations'
    print ''
    start_cmsDriver_time = time.time()
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
            exitCode, output, error = executeCommand(sample['command'])
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
            msg += 'Run: %s\n' % (sample['inputData'].get('RUN', 'All'))
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
                #  //
                # // Skip HARVESTING cmsDriver commands
                #//
                if steps[step]['skipStep']:
                    print 'This is a HARVESTING cmsDriver command, skipping. '
                    continue
                exitCode, output, error = executeCommand(steps[step]['command'])
                if exitCode == 0:
                    print 'cmsDriver command for step %s to produce:' % i, \
                        steps[step]['outputName'], \
                        'exited with ExitCode:', exitCode
                else:
                    print 'cmsDriver command for step %s to produce:' % i, \
                        steps[step]['outputName'], \
                        'failed with ExitCode:', exitCode
                    sys.exit(1)

    cmsDriver_time = time.time() - start_cmsDriver_time

    print ''
    print 'Workflow creation'
    print ''
    start_workflow_time = time.time()

    datasets = []
    unmergedDatasets = []
    mergedDatasets = []
    workflows = {}
    
    #  //
    # // Create workflows
    #//
    for sample in samples:
        command = 'python ' + scriptsDir
        conditions = '' # Conditions -> processingString
        #  //
        # // In case we are processing data
        #//
        if sample['isRealData']:
            command += '/createProcessingWorkflow.py \\\n'
            # Not changing the primary dataset name for real data.
            command += '--override-channel=' + sample['primary'] + ' \\\n'
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
            i = 0
            for step in sample['steps']:
                # Is this a HARVESTING step? If so, skip it!
                if steps[step]['skipStep']:
                    continue
                # Not a HARVESTING step, continue normally.
                command += '--version=' + version + ' \\\n'
                command += '--py-cfg=' + steps[step]['outputName'] + ' \\\n'
                if i != 0 or not sample['isRealData']:
                    command += '--stageout-intermediates=%s \\\n' % (
                        steps[step]['stagePrevious'])
                    command += '--chained-input=output \\\n'
                else:
                    dqmScenario = steps[step]['DQMData']['Scenario']
                #  //
                # // If a two-hlt tables workflow, will take conditions from
                #// the second step information
                #\\
                if not steps[step]['stagePrevious'] and \
                    i == 0:
                    conditions = steps[step]['conditions']
                i += 1
        #  //
        # // Common options
        #//
        command += '--group=RelVal \\\n'
        command += '--category=relval \\\n'
        command += '--activity=RelVal \\\n'
        command += '--acquisition_era=' + sample['AcqEra'] + ' \\\n'
        command += '--only-sites=' + onlySites + ' \\\n'
        command += '--processing_version=' + processing_version + ' \\\n'
        # Workflow label
        if workflow_label:
            command += '--workflow_tag=' + workflow_label + ' \\\n'
        #  //
        # // processingString="CMSSWVersion"_"Conditions"_"specialTag"_"extra-label"
        #// CMSSWVersion is appended only when the input dataset does not have it.
        #\\
        processing_string_parts = []
        if sample['AcqEra'] != version:
            processing_string_parts.append(version)
        processing_string_parts.append(conditions)
        if sample['specialTag']:
            processing_string_parts.append(sample['specialTag'])
        if extra_label:
            processing_string_parts.append(extra_label)
        command += '--processing_string=' + "_".join(processing_string_parts)

        if debug:
            print command
            print ''

        start_command_time = time.time()
        exitCode, output, error = executeCommand(command)
        command_time = time.time() - start_command_time

        if debug:
            print output
            print ''
        output = [x for x in output.split('\n') if x]

        if exitCode == 0:
            #parse output
            tmp = []
            index = FindIndex(output,'Output Datasets')
            for dataset in output[index+1:]:
                tmp.append(dataset.strip())
            # DQM Data
            dqmInfo = {}
            dqmInfo['Runs'] = sample['DQMData']['Runs']
            if sample['isRealData']:
                dqmInfo['Scenario'] = dqmScenario
            else:
                dqmInfo['Scenario'] = sample['DQMData']['Scenario']
                    
            datasets.append({'unmerged': tmp,
                            'totalEvents': sample['totalEvents'],
                            'merged': [x.replace('-unmerged','') for x in tmp],
                            'DQMData': dqmInfo
                            })
            unmergedDatasets.append(tmp)
            index = FindIndex(output,'Created')
            if index == -1:
                print "No workflow was created by create*workflow.py"
                sys.exit(1)
            workflow = output[index].split()[1].strip()
            workflows.setdefault(workflow, {})['isRealData'] = sample['isRealData']
            workflows[workflow]['time'] = command_time
            print 'workflow creation command for workflow:', workflow, \
                'exited with ExitCode:', exitCode
        else :
            print 'workflow creation command:'
            print command
            print 'failed: %s' % error
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

    workflow_time = time.time() - start_workflow_time

    print ''
    print 'Write helper scripts'
    print ''

    # WorkflowInjector:Input script
    inputScript = open('input.sh','w')
    inputScript.write('#!/bin/bash\n')
    feeder = 'None'
    for workflow in workflows.keys():
        if workflows[workflow]['isRealData']:
            if feeder.find('ReReco') < 0:
                inputScript.write('python $PRODAGENT_ROOT/util/publish.py WorkflowInjector:SetPlugin BlockFeeder\n')
                feeder = 'ReReco'
        else :
            if feeder.find('Request') < 0:
                inputScript.write('python $PRODAGENT_ROOT/util/publish.py WorkflowInjector:SetPlugin RequestFeeder\n')
                feeder = 'Request'
        inputScript.write('python $PRODAGENT_ROOT/util/publish.py WorkflowInjector:Input ' + os.path.join(os.getcwd(), workflow) + '\n')
    inputScript.close()
    os.chmod('input.sh',0755)
    print 'Wrote WorkflowInjector:Input script to:',os.path.join(os.getcwd(),'input.sh') 

    # ForceMerge
    forceMergeScript = open('forceMerge.sh','w')
    forceMergeScript.write('#!/bin/bash\n')
    for sample in unmergedDatasets :
        for dataset in sample :
            forceMergeScript.write('python $PRODAGENT_ROOT/util/publish.py ForceMerge ' + dataset + '\n')
    forceMergeScript.close()
    os.chmod('forceMerge.sh',0755)
    print 'Wrote ForceMerge script to:',os.path.join(os.getcwd(),'forceMerge.sh')

    # MigrateDatasetToGlobal
    migrateScript = open('migrateToGlobal.sh','w')
    migrateScript.write('#!/bin/bash\n')
    for sample in mergedDatasets :
        for dataset in sample :
            migrateScript.write('python $PRODAGENT_ROOT/util/publish.py DBSInterface:MigrateDatasetToGlobal ' + dataset + '\n')
    migrateScript.close()
    os.chmod('migrateToGlobal.sh',0755)
    print 'Wrote DBSInterface:MigrateDatasetToGlobal script to:',os.path.join(os.getcwd(),'migrateToGlobal.sh')

    # PhEDExInjectDataset
    phedexScript = open('injectIntoPhEDEx.sh','w')
    phedexScript.write('#!/bin/bash\n')
    for sample in mergedDatasets :
        for dataset in sample :
            phedexScript.write('python $PRODAGENT_ROOT/util/publish.py PhEDExInjectDataset ' + dataset + '\n')
    phedexScript.close()
    os.chmod('injectIntoPhEDEx.sh',0755)
    print 'Wrote PhEDExInjectDataset script to:',os.path.join(os.getcwd(),'injectIntoPhEDEx.sh')

    # DBS: query unmerged datasets
    queryUnmergedScript = open('queryUnmerged.sh','w')
    queryUnmergedScript.write('#!/bin/bash\n')
    for sample in unmergedDatasets :
        for dataset in sample :
            #if dataset.find('-RECO') == -1 or len(sample) == 1 :
            queryUnmergedScript.write('python $PRODAGENT_ROOT/util/InspectDBS2.py --DBSURL=' + DBSURL  + ' --datasetPath=' + dataset + ' | grep total\n')
    queryUnmergedScript.close()
    os.chmod('queryUnmerged.sh',0755)
    print 'Wrote DBS query script for unmerged datasets to:',os.path.join(os.getcwd(),'queryUnmerged.sh')

    # DBS: query merged datasets
    queryMergedScript = open('queryMerged.sh','w')
    queryMergedScript.write('#!/bin/bash\n')
    for sample in mergedDatasets :
        for dataset in sample :
            #if dataset.find('-RECO') == -1 or len(sample) == 1 :
            queryMergedScript.write('python $PRODAGENT_ROOT/util/InspectDBS2.py --DBSURL=' + DBSURL  + ' --datasetPath=' + dataset + ' | grep total\n')
    queryMergedScript.close()
    os.chmod('queryMerged.sh',0755)
    print 'Wrote DBS query script for merged datasets to:',os.path.join(os.getcwd(),'queryMerged.sh')

    # DQMHarvesting
    DQMinputScript = open('DQMinput.sh','w')
    DQMinputScript.write("#!/bin/bash\n")
    reHarvest = re.compile(r'/.*/.*/(RECO|.*-RECO)') # Only RECO datasets for now.
    for sample in datasets:
        for dataset in sample['merged']:
            if reHarvest.match(dataset):
                for run in sample['DQMData']['Runs'].split(","):
                    DQMinputScript.write('python $PRODAGENT_ROOT/util/harvestDQM.py --run=%s --path=%s --scenario=%s\n' % (
                    run, dataset, sample['DQMData']['Scenario']))
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

    total_time = time.time() - start_total_time

    # File with timing report (Parsing, cmsDriver comands, workflow creation)
    timingInfo = open('timingInfo.txt', 'w')
    timingInfo.write('Total time: %s s\n' % total_time)
    timingInfo.write('Cofigs. creation time: %s s\n' % cmsDriver_time)
    timingInfo.write('Workflows creation time: %s s\n' % workflow_time)
    output_text = []
    sum = 0
    for workflow in workflows:
        if sum == 0:
            min = [workflow, workflows[workflow]['time']]
            max = [workflow, workflows[workflow]['time']]
        sum += workflows[workflow]['time']
        output_text.append("%s: %s s" % (workflow, workflows[workflow]['time']))
        if max[1] < workflows[workflow]['time']:
            max = [workflow, workflows[workflow]['time']]
        if min[1] > workflows[workflow]['time']:
            min = [workflow, workflows[workflow]['time']]
    timingInfo.write('Average time per workflow: %s s\n' % (int(sum) / int(len(workflows))))
    timingInfo.write('Max. time on %s: %s s\n' % tuple(max))
    timingInfo.write('Min. time on %s: %s s\n' % tuple(min))
    timingInfo.write('=' * 10)
    timingInfo.write('Details of time per workflow:\n%s\n' % "\n".join(output_text))


def FindIndex(output, string):
    """
    Given a list of string, it find the list index where the string is
    contained.
    """
    index = -1
    counter = 0
    for field in output:
        if field.find(string) != -1 : index = counter
        counter += 1
    return index


def getDQMScenario(cmsDriverCmd):
    """
    Returns the scenario to use for DQM harvesting depending on the cmsDriver
    command received as input argument
    """
    # FastSim
    if cmsDriverCmd.count('FASTSIM'):
        return 'relvalmcfs'

    cmsDriverCmdParts = cmsDriverCmd.split()
    if cmsDriverCmdParts.count('--scenario'):
        scenario = cmsDriverCmdParts[cmsDriverCmdParts.index('--scenario') + 1]

    # RealData
    if cmsDriverCmdParts.count('--data'):
        if scenario in ('cosmics', 'pp'):
            return scenario
        else:
            return 'pp'

    # If I am here, it's relvalmc
    return 'relvalmc'


def executeCommand(cmd):
    """
    Uses subprocess module for executing a command in a subshell
    """
    popen = subprocess.Popen(cmd,
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    exitCode = popen.wait()
    (stdout, stderr) = popen.communicate()
    return exitCode, stdout, stderr
    

def DBSXMLParser(xml_string):
    """
    Rertuns a list of dictionaries. Where each row is the result from a DBS
    query. The dictionary keys are the column names.
    """
    # xml parser
    results = []
    class Handler(xml.sax.handler.ContentHandler):
        def __init__(self):
            xml.sax.handler.ContentHandler.__init__(self)
            self.buffer = ''
            self.result = {}
            self.in_row = False
            self.current_item = ''
        def startElement(self, name, attrs):
            if name == 'row':
                self.in_row = True
                self.result = {}
            elif self.in_row and not self.current_item:
                self.current_item = name
                self.buffer = ''
        def characters(self, s):
            if str(escape(s)).strip() in ('', '\n'):
                return
            if self.in_row and self.current_item:
                self.buffer += str(escape(s))
        def endElement(self, name):
            if name == 'row':
                self.in_row = False
                results.append(self.result)
            elif self.in_row and name == self.current_item:
                self.result[self.current_item] = self.buffer
                self.current_item = ''

    xml.sax.parseString(xml_string, Handler())
    return results


################
if __name__ == '__main__':
    main(sys.argv[1:])
