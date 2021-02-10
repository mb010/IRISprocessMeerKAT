import os,sys

import globals
from paths import *
import config_parser
import logger
from kwarg_tools import get_config_kwargs

# ========================================================================================================

def write_jdl(config,parametric=True):
    
    iris_kwargs = get_config_kwargs(config, 'iris', globals.IRIS_CONFIG_KEYS)
    data_kwargs = get_config_kwargs(config, 'data', ['vis'])
    
    ntag = len(iris_kwargs['tags'])
    taglist = "{"
    for tag in range(ntag-1):
        taglist+= "'"+iris_kwargs['tags'][tag]+"', "
    taglist+= "'"+iris_kwargs['tags'][ntag-1]+"'}"

    inputdata = "{'"+data_kwargs['vis']+"'}"
    outputdata = "{'"+iris_kwargs['outpath']+"outputMMS_%j.tar.gz','"+iris_kwargs['outpath']+"images_%j.tar.gz','"+iris_kwargs['outpath']+"caltables_%j.tar.gz'}"

    spwlist = "{'880.0~930.0MHz','930.0~980.0MHz'}"
    
    jdl = """[
        JobName = "pipeline_spw_%n";"""
    jdl+="""
        InputSandbox = {"run_process_meerkat.sh","cal_scripts.tar.gz","myconfig.txt","config_parser.py","processMeerKAT.py"};"""
    jdl+="""
        InputData = {0};\n""".format(inputdata)
    jdl+="""
        Tags = {0};\n""".format(taglist)
    jdl+="""
        Executable = "run_process_meerkat.sh";
        Site = "{site}";
        Arguments = "%n %j %s";
        Platform = "{platform}";\n""".format(**iris_kwargs)
    jdl+="""
        Parameters = {0};\n""".format(spwlist)
    jdl+="""
        OutputSE = "{outputse}";""".format(**iris_kwargs)
    jdl+="""
        OutputData = {0};""".format(outputdata)
    jdl+="""
        OutputSandbox = {"StdOut", "StdErr", "listobs_%j.txt"};
        StdOutput = "StdOut";
        StdError = "StdErr";
        ]"""
    
    return jdl

# ========================================================================================================

def write_command(script, container, config, flag):
    
    params={}
    params['script'] = script
    params['container'] = container.split('/')[-1]
    params['config'] = config.split('/')[-1]
    
    command = """\n>>> executing {script} on data \n""".format(**params)
    if flag=="mpi":
        command+= """singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C {container} mpicasa -n $OMP_NUM_THREADS casa --log2term -c {script} --config {config} \n""".format(**params)
    elif flag=="plot":
        command+= """singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C {container} xvfb-run -d casa --log2term -c {script} --config {config} \n""".format(**params)
    else:
        command+= """singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C {container} casa --log2term -c {script} --config {config} \n""".format(**params)
    
    return command

# ========================================================================================================

def write_runscript(config, scripts=[], threadsafe=[], containers=[], num_precal_scripts=0, outputse = '', tags='', site='', platform = '', outpath='', name='', verbose=False, quiet=False, timestamp=''):
    
    """Write a series of commands to calibrate a CASA MS"""
    
    #kwargs = locals()
    iris_kwargs = get_config_kwargs(config, 'iris', globals.IRIS_CONFIG_KEYS)
    crosscal_kwargs = get_config_kwargs(config, 'crosscal', globals.CROSSCAL_CONFIG_KEYS)
    
    
    runscript="""#!/bin/bash
        echo "==START=="
        /bin/hostname
        echo "======="
        /bin/ls -la
        echo "======="
        /bin/date
        echo "======="
        
        echo "kernel version check:"
        uname -r
        echo "Script: $0"
        echo "Realisation: $1"
        echo "JobID: $2"
        echo "SPW: $3"
        
        echo "No. threads: "
        echo $OMP_NUM_THREADS
        
        echo "printing singularity version on grid:"
        singularity --version
        
        # ========================================================
        mkdir data
        /bin/mv *.ms.tar.gz data
        cd data
        echo ">>> extracting data"
        COMMAND="ls *.ms.tar.gz"
        for FILE in `eval $COMMAND`
        do
        tar -xzvf $FILE
        done
        /bin/ls -la
        cd ..
        echo ">>> data set successfully extracted"
        # ========================================================
        
        tar -xzvf cal_scripts.tar.gz"""
    
    
    # Write commandline for each input python script
    for i,script in enumerate(scripts):
        
        container = iris_kwargs['container']
        
        if threadsafe[i]=="True":
            flag="mpi"
        elif ('plot' in script):
            flag="plot"
        else:
            flag="none"
        
        command = write_command(script, container, config, flag)
        runscript += command
    
    
    runscript += """\n
        cp myconfig.txt myconfig_$2.txt
        
        tar -czvf outputMMS_$2.tar.gz *mms*
        tar -czvf images_$2.tar.gz images
        tar -czvf plots_$2.tar.gz plots
        tar -czvf caltables_$2.tar.gz caltables"""
    
    runfile = '{0}'.format(globals.SCRIPTNAME)
    run_script = open(runfile,'w')
    run_script.write(runscript)
    run_script.close()
    
    if crosscal_kwargs['nspw'] > 1:
        # Write parametric JDL
        jdl = write_jdl(config,parametric=True)
    else:
        # Write standard JDL
        jdl = write_jdl(config,parametric=False)
    
    jdlfile = '{0}'.format(globals.JDLNAME)
    jdl_script = open(jdlfile,'w')
    jdl_script.write(jdl)
    jdl_script.close()

    return

# ========================================================================================================

def write_jobs_iris(config, scripts=[], threadsafe=[], containers=[], num_precal_scripts=0, outputse = '', tags='', site='', platform = '', outpath='', name='', verbose=False, quiet=False, timestamp=''):
    
    # write precal runscript & jdl
    
    # write calibration runscript & jdl

    return

# ========================================================================================================

