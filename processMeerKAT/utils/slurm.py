import os,sys

import globals
from paths import *
import config_parser
import logger
from kwarg_tools import get_config_kwargs

# ========================================================================================================
# ========================================================================================================


def get_slurm_dict(arg_dict,slurm_config_keys):
    
    """Build a slurm dictionary to be inserted into config file, using specified keys.
        
        Arguments:
        ----------
        arg_dict : dict
        Dictionary of arguments passed into this script, which is inserted into the config file under section [slurm].
        slurm_config_keys : list
        List of keys from arg_dict to insert into config file.
        
        Returns:
        --------
        slurm_dict : dict
        Dictionary to insert into config file under section [slurm]."""
    
    slurm_dict = {key:arg_dict[key] for key in slurm_config_keys}
    return slurm_dict

# ========================================================================================================

def srun(arg_dict,qos=True,time=10,mem=4):
    
    """Return srun call, with certain parameters appended.
        
        Arguments:
        ----------
        arg_dict : dict
        Dictionary of arguments passed into this script, which is used to append parameters to srun call.
        qos : bool, optional
        Quality of service, set to True for interactive jobs, to increase likelihood of scheduling.
        mem : int, optional
        The memory in GB (per node) to use for this call.
        time : str, optional
        Time limit to use for this call, in the form d-hh:mm:ss.
        
        Returns:
        --------
        call : str
        srun call with arguments appended."""
    
    call = 'srun --time={0} --mem={1}GB --partition={2}'.format(time,mem,arg_dict['partition'])
    if qos:
        call += ' --qos qos-interactive'
    if arg_dict['exclude'] != '':
        call += ' --exclude={0}'.format(arg_dict['exclude'])
    if arg_dict['reservation'] != '':
        call += ' --reservation={0}'.format(arg_dict['reservation'])
    
    return call

# ========================================================================================================

def write_command(script, args, name='job', mpi_wrapper=globals.MPI_WRAPPER, container=globals.CONTAINER, casa_script=True, casacore=False, logfile=True, plot=False, SPWs='', nspw=1):
    
    """Write bash command to call a script (with args) directly with srun, or within sbatch file, optionally via CASA.
        
        Arguments:
        ----------
        script : str
        Path to script called (assumed to exist or be in PATH or calibration scripts directory).
        args : str
        Arguments to pass into script. Use '' for no arguments.
        name : str, optional
        Name of this job, to append to CASA output name.
        mpi_wrapper : str, optional
        MPI wrapper for this job. e.g. 'srun', 'mpirun', 'mpicasa' (may need to specify path).
        container : str, optional
        Path to singularity container used for this job.
        casa_script : bool, optional
        Is the script that is called within this job a CASA script?
        casacore : bool, optional
        Is the script that is called within this job a casacore script?
        logfile : bool, optional
        Write the CASA output to a log file? Only used if casa_script==True.
        plot : bool, optional
        This job is a plotting task that needs to call xvfb-run.
        SPWs : str, optional
        Comma-separated list of spw ranges.
        nspw : int, optional
        Number of spectral windows.
        
        Returns:
        --------
        command : str
        Bash command to call with srun or within sbatch file."""
    
    arrayJob = ',' in SPWs and 'partition' in script and nspw > 1
    
    #Store parameters passed into this function as dictionary, and add to it
    params = locals()
    params['LOG_DIR'] = globals.LOG_DIR
    params['job'] = '${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}' if arrayJob else '${SLURM_JOB_ID}'
    params['job'] = '${SLURM_JOB_NAME}-' + params['job']
    params['casa_call'] = ''
    params['casa_log'] = '--nologfile'
    params['plot_call'] = ''
    command = ''
    
    #If script path doesn't exist and is not in user's bash path, assume it's in the calibration scripts directory
    if not os.path.exists(script):
        params['script'] = check_path(script, update=True)
    
    #If specified by user, call script via CASA, call with xvfb-run, and write output to log file
    if plot:
        params['plot_call'] = 'xvfb-run -a'
    if logfile:
        params['casa_log'] = '--logfile {LOG_DIR}/{job}.casa'.format(**params)
    if casa_script:
        params['casa_call'] = "{plot_call} casa --nologger --nogui {casa_log} -c".format(**params)
    if casacore:
        params['singularity_call'] = 'run' #points to python that comes with CASA, including casac
    else:
        params['singularity_call'] = 'exec'
    if not casa_script and not casacore:
        params['casa_call'] = 'python'

    if arrayJob:
        command += """#Iterate over SPWs in job array, launching one after the other
            SPWs="%s"
            arr=($SPWs)
            cd ${arr[SLURM_ARRAY_TASK_ID]}
            
            """ % SPWs.replace(',',' ').replace('0:','')

    command += "{mpi_wrapper} singularity {singularity_call} {container} {casa_call} {script} {args}".format(**params)

    #Get rid of annoying msmd output from casacore call
    if casacore:
        command += " 2>&1 | grep -v 'msmetadata_cmpt.cc::open\|MSMetaData::_computeScanAndSubScanProperties\|MeasIERS::fillMeas(MeasIERS::Files, Double)\|Position:'"
    if arrayJob:
        command += '\ncd ..\n'

    return command


# ========================================================================================================

def write_jobs(config, scripts=[], threadsafe=[], containers=[], num_precal_scripts=0, mpi_wrapper=globals.MPI_WRAPPER, nodes=8, ntasks_per_node=4, mem=globals.MEM_PER_NODE_GB_LIMIT,plane=1,
               partition='Main', time='12:00:00', submit=False, name='', verbose=False, quiet=False, dependencies='', exclude='', account='b03-idia-ag', reservation='', timestamp=''):
    
    """Write a series of sbatch job files to calibrate a CASA measurement set.
        
        Arguments:
        ----------
        config : str
        Path to config file.
        scripts : list (of paths), optional
        List of paths to scripts (assumed to be python -- i.e. extension .py) to call within seperate sbatch jobs.
        threadsafe : list (of bools), optional
        Are these scripts threadsafe (for MPI)? List assumed to be same length as scripts.
        containers : list (of paths), optional
        List of paths to singularity containers to use for each script. List assumed to be same length as scripts.
        num_precal_scripts : int, optional
        Number of precal scripts.
        mpi_wrapper : str, optional
        Path to MPI wrapper to use for threadsafe tasks (otherwise srun used).
        nodes : int, optional
        Number of nodes to use for this job.
        tasks : int, optional
        The number of tasks per node to use for this job.
        mem : int, optional
        The memory in GB (per node) to use for this job.
        plane : int, optional
        Distrubute tasks for this job using this block size before moving onto next node.
        partition : str, optional
        SLURM partition to use (default: "Main").
        time : str, optional
        Time limit to use for all jobs, in the form d-hh:mm:ss.
        submit : bool, optional
        Submit jobs to SLURM queue immediately?
        name : str, optional
        Unique name to give this pipeline run, appended to the start of all job names.
        verbose : bool, optional
        Verbose output?
        quiet : bool, optional
        Activate quiet mode, with suppressed output?
        dependencies : str, optional
        Comma-separated list of SLURM job dependencies.
        exclude : str, optional
        SLURM worker nodes to exclude.
        account : str, optional
        SLURM accounting group for sbatch jobs.
        reservation : str, optional
        SLURM reservation to use.
        timestamp : str, optional
        Timestamp to put on this run and related runs in SPW directories."""
    
    kwargs = locals()
    crosscal_kwargs = get_config_kwargs(config, 'crosscal', globals.CROSSCAL_CONFIG_KEYS)
    pad_length = len(name)
    
    #Write sbatch file for each input python script
    for i,script in enumerate(scripts):
        jobname = os.path.splitext(os.path.split(script)[1])[0]
        
        #Use input SLURM configuration for threadsafe tasks, otherwise call srun with single node and single thread
        if threadsafe[i]:
            write_sbatch(script,'--config {0}'.format(globals.TMP_CONFIG),nodes=nodes,tasks=ntasks_per_node,mem=mem,plane=plane,exclude=exclude,mpi_wrapper=mpi_wrapper,
                         container=containers[i],partition=partition,time=time,name=jobname,runname=name,SPWs=crosscal_kwargs['spw'],nspw=crosscal_kwargs['nspw'],account=account,reservation=reservation)
        else:
            write_sbatch(script,'--config {0}'.format(globals.TMP_CONFIG),nodes=1,tasks=1,mem=mem,plane=1,mpi_wrapper='srun',container=containers[i],
                         partition=partition,time=time,name=jobname,runname=name,SPWs=crosscal_kwargs['spw'],nspw=crosscal_kwargs['nspw'],exclude=exclude,account=account,reservation=reservation)

    #Replace all .py with .sbatch
    scripts = [os.path.split(scripts[i])[1].replace('.py','.sbatch') for i in range(len(scripts))]
    precal_scripts = scripts[:num_precal_scripts]
    postcal_scripts = scripts[num_precal_scripts:]
    echo = False if quiet else True
    
    if crosscal_kwargs['nspw'] > 1:
        #Build master master script, calling each of the separate SPWs at once, precal scripts before this, and postcal scripts after this
        write_spw_master(globals.MASTER_SCRIPT,config,SPWs=crosscal_kwargs['spw'],precal_scripts=precal_scripts,postcal_scripts=postcal_scripts,submit=submit,pad_length=pad_length,dependencies=dependencies,timestamp=timestamp,slurm_kwargs=kwargs)
    else:
        #Build master pipeline submission script
        write_master(globals.MASTER_SCRIPT,config,scripts=scripts,submit=submit,pad_length=pad_length,verbose=verbose,echo=echo,dependencies=dependencies,slurm_kwargs=kwargs)


# ========================================================================================================

def write_sbatch(script,args,nodes=1,tasks=16,mem=globals.MEM_PER_NODE_GB_LIMIT,name="job",runname='',plane=1,exclude='',mpi_wrapper=globals.MPI_WRAPPER,
                 container=globals.CONTAINER,partition="Main",time="12:00:00",casa_script=True,casacore=False,SPWs='',nspw=1,account='b03-idia-ag',reservation=''):
    
    """Write a SLURM sbatch file calling a certain script (and args) with a particular configuration.
        
        Arguments:
        ----------
        script : str
        Path to script called within sbatch file (assumed to exist or be in PATH or calibration directory).
        args : str
        Arguments passed into script called within this sbatch file. Use '' for no arguments.
        time : str, optional
        Time limit on this job.
        nodes : int, optional
        Number of nodes to use for this job.
        tasks : int, optional
        The number of tasks per node to use for this job.
        mem : int, optional
        The memory in GB (per node) to use for this job.
        name : str, optional
        Name for this job, used in naming the various output files.
        runname : str, optional
        Unique name to give this pipeline run, appended to the start of all job names.
        plane : int, optional
        Distrubute tasks for this job using this block size before moving onto next node.
        exclude : str, optional
        SLURM worker nodes to exclude.
        mpi_wrapper : str, optional
        MPI wrapper for this job. e.g. 'srun', 'mpirun', 'mpicasa' (may need to specify path).
        container : str, optional
        Path to singularity container used for this job.
        partition : str, optional
        SLURM partition to use (default: "Main").
        time : str, optional
        Time limit to use for this job, in the form d-hh:mm:ss.
        casa_script : bool, optional
        Is the script that is called within this job a CASA script?
        casacore : bool, optional
        Is the script that is called within this job a casacore script?
        SPWs : str, optional
        Comma-separated list of spw ranges.
        nspw : int, optional
        Number of spectral windows.
        account : str, optional
        SLURM accounting group for sbatch jobs.
        reservation : str, optional
        SLURM reservation to use."""
    
    if not os.path.exists(globals.LOG_DIR):
        os.mkdir(globals.LOG_DIR)

    #Store parameters passed into this function as dictionary, and add to it
    params = locals()
    params['LOG_DIR'] = globals.LOG_DIR

    #Use multiple CPUs for tclean and paratition scripts
    params['cpus'] = 1
    if 'tclean' in script or 'selfcal' in script or 'bdsf' in script or 'partition' in script:
        params['cpus'] = int(globals.CPUS_PER_NODE_LIMIT/tasks)
    #hard-code for 2/4 polarisations
    if 'partition' in script:
        dopol = config_parser.get_key(globals.TMP_CONFIG, 'run', 'dopol')
        if dopol and 4*tasks < globals.CPUS_PER_NODE_LIMIT:
            params['cpus'] = 4
        elif not dopol and params['cpus'] > 2:
            params['cpus'] = 2

    #If requesting all CPUs, user may as well use all memory
    if params['cpus'] * tasks == globals.CPUS_PER_NODE_LIMIT:
        params['mem'] = globals.MEM_PER_NODE_GB_LIMIT
    
    #Use xvfb for plotting scripts, casacore for validate_input.py, and just python for run_bdsf.py
    plot = ('plot' in script)
    if script == 'validate_input.py':
        casa_script = False
        casacore = True
    elif script == 'run_bdsf.py':
        casa_script = False
        casacore = False

    params['command'] = write_command(script,args,name=name,mpi_wrapper=mpi_wrapper,container=container,casa_script=casa_script,plot=plot,SPWs=SPWs,nspw=nspw,casacore=casacore)
    if 'partition' in script and ',' in SPWs and nspw > 1:
        params['ID'] = '%A_%a'
        params['array'] = '\n#SBATCH --array=0-{0}%4'.format(nspw-1)
    else:
        params['ID'] = '%j'
        params['array'] = ''
    params['exclude'] = '\n#SBATCH --exclude={0}'.format(exclude) if exclude != '' else ''
    params['reservation'] = '\n#SBATCH --reservation={0}'.format(reservation) if reservation != '' else ''

    contents = """#!/bin/bash{array}{exclude}{reservation}
    #SBATCH --account={account}
    #SBATCH --nodes={nodes}
    #SBATCH --ntasks-per-node={tasks}
    #SBATCH --cpus-per-task={cpus}
    #SBATCH --mem={mem}GB
    #SBATCH --job-name={runname}{name}
    #SBATCH --distribution=plane={plane}
    #SBATCH --output={LOG_DIR}/%x-{ID}.out
    #SBATCH --error={LOG_DIR}/%x-{ID}.err
    #SBATCH --partition={partition}
    #SBATCH --time={time}
    
    export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
    
    {command}"""

    #insert arguments and remove whitespace
    contents = contents.format(**params).replace("    ","")

    #write sbatch file
    sbatch = '{0}.sbatch'.format(name)
    config = open(sbatch,'w')
    config.write(contents)
    config.close()
    
    logger.logger.debug('Wrote sbatch file "{0}"'.format(sbatch))


# ========================================================================================================

def write_spw_master(filename,config,SPWs,precal_scripts,postcal_scripts,submit,dir='jobScripts',pad_length=5,dependencies='',timestamp='',slurm_kwargs={}):
    
    """Write master master script, which separately calls each of the master scripts in each SPW directory.
        
        filename : str
        Name of master pipeline submission script.
        config : str
        Path to config file.
        SPWs : str
        Comma-separated list of spw ranges.
        precal_scripts : list, optional
        List of sbatch scripts to call in order, before running pipeline in SPW directories.
        postcal_scripts : list, optional
        List of sbatch scripts to call in order, after running pipeline in SPW directories.
        submit : bool, optional
        Submit jobs to SLURM queue immediately?
        dir : str, optional
        Name of directory to output ancillary job scripts.
        pad_length : int, optional
        Length to pad the SLURM sacct output columns.
        dependencies : str, optional
        Comma-separated list of SLURM job dependencies.
        timestamp : str, optional
        Timestamp to put on this run and related runs in SPW directories.
        slurm_kwargs : list, optional
        Parameters parsed from [slurm] section of config."""
    
    master = open(filename,'w')
    master.write('#!/bin/bash\n')
    SPWs = SPWs.replace('0:','')
    toplevel = len(precal_scripts + postcal_scripts) > 0
    
    scripts = precal_scripts[:]
    if len(scripts) > 0:
        command = 'sbatch'
        if dependencies != '':
            master.write('\n#Run after these dependencies\nDep={0}\n'.format(dependencies))
            command += ' -d afterok:$Dep --kill-on-invalid-dep=yes'
            dependencies = '' #Remove dependencies so it isn't fed into launching SPW scripts
        master.write('\n#{0}\n'.format(scripts[0]))
        master.write("allSPWIDs=$({0} {1} | cut -d ' ' -f4)\n".format(command,scripts[0]))
        scripts.pop(0)
    for script in scripts:
        command = 'sbatch -d afterok:$allSPWIDs --kill-on-invalid-dep=yes'
        master.write('\n#{0}\n'.format(script))
        master.write("allSPWIDs+=,$({0} {1} | cut -d ' ' -f4)\n".format(command,script))
    
    if 'calc_refant.sbatch' in precal_scripts:
        master.write('echo Calculating reference antenna, and copying result to SPW directories.\n')
    if 'partition.sbatch' in precal_scripts:
        master.write('echo Running partition job array, iterating over {0} SPWs.\n'.format(len(SPWs.split(','))))
    
    partition = len(precal_scripts) > 0 and 'partition' in precal_scripts[-1]
    if partition:
        master.write('\npartitionID=$(echo $allSPWIDs | cut -d , -f{0})\n'.format(len(precal_scripts)))

    #Add time as extn to this pipeline run, to give unique filenames
    killScript = 'killJobs'
    summaryScript = 'summary'
    fullSummaryScript = 'fullSummary'
    errorScript = 'findErrors'
    timingScript = 'displayTimes'
    cleanupScript = 'cleanup'

    master.write('\n#Add time as extn to this pipeline run, to give unique filenames')
    master.write("\nDATE={0}\n".format(timestamp))
    master.write('mkdir -p {0}\n'.format(dir))
    master.write('mkdir -p {0}\n\n'.format(globals.LOG_DIR))
    extn = '_$DATE.sh'
    
    for i,spw in enumerate(SPWs.split(',')):
        master.write('echo Running pipeline in directory "{0}" for spectral window 0:{0}\n'.format(spw))
        master.write('cd {0}\n'.format(spw))
        master.write('output=$({0} --config ./{1} --run --submit --quiet'.format(os.path.split(globals.THIS_PROG)[1],config))
        if partition:
            master.write(' --dependencies=$partitionID\_{0}'.format(i))
        elif len(precal_scripts) > 0:
            master.write(' --dependencies=$allSPWIDs')
        elif dependencies != '':
            master.write(' --dependencies={0}'.format(dependencies))
        master.write(')\necho $output\n')
        if i == 0:
            master.write("IDs=$(echo $output | cut -d ' ' -f7)")
        else:
            master.write("IDs+=,$(echo $output | cut -d ' ' -f7)")
        master.write('\ncd ..\n\n')
    
    if 'concat.sbatch' in postcal_scripts:
        master.write('echo Will concatenate MSs/MMSs and create quick-look continuum cube across all SPWs for all fields from \"{0}\".\n'.format(config))
    scripts = postcal_scripts[:]

    #Hack to perform correct number of selfcal loops
    if 'selfcal_part1.sbatch' in scripts and 'selfcal_part2.sbatch' in scripts and 'run_bdsf.sbatch' in scripts and 'make_pixmask.sbatch' in scripts:
        selfcal_loops = config_parser.parse_config(config)[0]['selfcal']['nloops']
        scripts.extend(['selfcal_part1.sbatch','selfcal_part2.sbatch','run_bdsf.sbatch','make_pixmask.sbatch']*(selfcal_loops))
        scripts.append('selfcal_part1.sbatch')

    if len(scripts) > 0:
        command = 'sbatch -d afterany:$IDs {0}'.format(scripts[0])
        master.write('\n#{0}\n'.format(scripts[0]))
        scripts.pop(0)
        if len(precal_scripts) == 0:
            master.write("allSPWIDs=$({0} | cut -d ' ' -f4)\n".format(command))
        else:
            master.write("allSPWIDs+=,$({0} | cut -d ' ' -f4)\n".format(command))
        for script in scripts:
            command = 'sbatch -d afterok:$allSPWIDs'
            master.write('\n#{0}\n'.format(script))
            master.write("allSPWIDs+=,$({0} {1} | cut -d ' ' -f4)\n".format(command,script))
    master.write('\necho Submitted the following jobIDs within the {0} SPW directories: $IDs\n'.format(len(SPWs.split(','))))

    prefix = ''
    #Write bash job scripts for the jobs run in this top level directory
    if toplevel:
        master.write('\necho Submitted the following jobIDs over all SPWs: $allSPWIDs\n')
        master.write('\necho For jobs over all SPWs:\n')
        prefix = 'allSPW_'
        write_all_bash_jobs_scripts(master,extn,IDs='allSPWIDs',dir=dir,prefix=prefix,pad_length=pad_length,slurm_kwargs=slurm_kwargs)
        master.write('\nln -f -s {1}{2}{3} {0}/{1}{4}{3}\n'.format(dir,prefix,summaryScript,extn,fullSummaryScript))

    master.write('\necho For all jobs within the {0} SPW directories:\n'.format(len(SPWs.split(','))))
    header = '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------' + '-'*pad_length
    do = """echo "for f in {%s,}; do if [ -d \$f ]; then cd \$f; ./%s/%s%s; cd ..; else echo Directory \$f doesn\\'t exist; fi; done;%s"""
    suffix = '' if toplevel else ' \"'
    write_bash_job_script(master, killScript, extn, do % (SPWs,dir,killScript,extn,suffix), 'kill all the jobs', dir=dir,prefix=prefix)
    write_bash_job_script(master, cleanupScript, extn, do % (SPWs,dir,cleanupScript,extn,' \"'), 'remove the MMSs/MSs within SPW directories \(after pipeline has run\), while leaving the concatenated data at the top level', dir=dir)
    
    do = """echo "counter=1; for f in {%s,}; do echo -n SPW \#\$counter:; echo -n ' '; if [ -d \$f ]; then cd \$f; pwd; ./%s/%s%s %s; cd ..; else echo Directory \$f doesn\\'t exist; fi; counter=\$((counter+1)); echo '%s'; done; """
    if toplevel:
        do += "echo -n 'All SPWs: '; pwd; "
    else:
        do += ' \"'
    write_bash_job_script(master, summaryScript, extn, do % (SPWs,dir,summaryScript,extn,"| grep -v 'PENDING\|COMPLETED'",header), 'view the progress \(for running or failed jobs\)', dir=dir,prefix=prefix)
    write_bash_job_script(master, fullSummaryScript, extn, do % (SPWs,dir,summaryScript,extn,'',header), 'view the progress \(for all jobs\)', dir=dir,prefix=prefix)
    header = '------------------------------------------------------------------------------------------' + '-'*pad_length
    write_bash_job_script(master, errorScript, extn, do % (SPWs,dir,errorScript,extn,'',header), 'find errors \(after pipeline has run\)', dir=dir,prefix=prefix)
    write_bash_job_script(master, timingScript, extn, do % (SPWs,dir,timingScript,extn,'',header), 'display start and end timestamps \(after pipeline has run\)', dir=dir,prefix=prefix)
    
    #Close master submission script and make executable
    master.close()
    os.chmod(filename, 509)
    
    #Submit script or output that it will not run
    if submit:
        logger.logger.info('Running master script "{0}"'.format(filename))
        os.system('./{0}'.format(filename))
    else:
        logger.logger.info('Master script "{0}" written, but will not run.'.format(filename))


# ========================================================================================================

def write_master(filename,config,scripts=[],submit=False,dir='jobScripts',pad_length=5,verbose=False, echo=True, dependencies='',slurm_kwargs={}):
    
    """Write master pipeline submission script, calling various sbatch files, and writing ancillary job scripts.
        
        Arguments:
        ----------
        filename : str
        Name of master pipeline submission script.
        config : str
        Path to config file.
        scripts : list, optional
        List of sbatch scripts to call in order.
        submit : bool, optional
        Submit jobs to SLURM queue immediately?
        dir : str, optional
        Name of directory to output ancillary job scripts.
        pad_length : int, optional
        Length to pad the SLURM sacct output columns.
        verbose : bool, optional
        Verbose output (inserted into master script)?
        echo : bool, optional
        Echo the pupose of each job script for the user?
        dependencies : str, optional
        Comma-separated list of SLURM job dependencies.
        slurm_kwargs : list, optional
        Parameters parsed from [slurm] section of config."""
    
    master = open(filename,'w')
    master.write('#!/bin/bash\n')
    timestamp = config_parser.get_key(config,'run','timestamp')
    if timestamp == '':
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        config_parser.overwrite_config(config, conf_dict={'timestamp' : "'{0}'".format(timestamp)}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
    
    #Copy config file to TMP_CONFIG and inform user
    if verbose:
        master.write("\necho Copying \'{0}\' to \'{1}\', and using this to run pipeline.\n".format(config,globals.TMP_CONFIG))
    master.write('cp {0} {1}\n'.format(config, TMP_CONFIG))

    #Hack to perform correct number of selfcal loops
    if 'selfcal_part1.sbatch' in scripts and 'selfcal_part2.sbatch' in scripts and 'run_bdsf.sbatch' in scripts and 'make_pixmask.sbatch' in scripts:
        selfcal_loops = config_parser.parse_config(config)[0]['selfcal']['nloops']
        scripts.extend(['selfcal_part1.sbatch','selfcal_part2.sbatch','run_bdsf.sbatch','make_pixmask.sbatch']*(selfcal_loops))
        scripts.append('selfcal_part1.sbatch')
    
    command = 'sbatch'
    
    if dependencies != '':
        master.write('\n#Run after these dependencies\nDep={0}\n'.format(dependencies))
        command += ' -d afterok:$Dep --kill-on-invalid-dep=yes'
    master.write('\n#{0}\n'.format(scripts[0]))
    if verbose:
        master.write('echo Submitting {0} SLURM queue with following command:\necho {1}\n'.format(scripts[0],command))
    master.write("IDs=$({0} {1} | cut -d ' ' -f4)\n".format(command,scripts[0]))
    scripts.pop(0)


    #Submit each script with dependency on all previous scripts, and extract job IDs
    for script in scripts:
        command = 'sbatch -d afterok:$IDs --kill-on-invalid-dep=yes'
        master.write('\n#{0}\n'.format(script))
        if verbose:
            master.write('echo Submitting {0} SLURM queue with following command\necho {1} {0}\n'.format(script,command))
        master.write("IDs+=,$({0} {1} | cut -d ' ' -f4)\n".format(command,script))

    master.write('\n#Output message and create {0} directory\n'.format(dir))
    master.write('echo Submitted sbatch jobs with following IDs: $IDs\n')
    master.write('mkdir -p {0}\n'.format(dir))

    #Add time as extn to this pipeline run, to give unique filenames
    master.write('\n#Add time as extn to this pipeline run, to give unique filenames')
    master.write("\nDATE={0}".format(timestamp))
    extn = '_$DATE.sh'
    
    #Copy contents of config file to jobScripts directory
    master.write('\n#Copy contents of config file to {0} directory\n'.format(dir))
    master.write('cp {0} {1}/{2}_$DATE.txt\n'.format(config,dir,os.path.splitext(config)[0]))
    
    #Write each job script - kill script, summary script, error script, and timing script
    write_all_bash_jobs_scripts(master,extn,IDs='IDs',dir=dir,echo=echo,pad_length=pad_length,slurm_kwargs=slurm_kwargs)
    
    #Close master submission script and make executable
    master.close()
    os.chmod(filename, 509)
    
    #Submit script or output that it will not run
    if submit:
        if echo:
            logger.logger.info('Running master script "{0}"'.format(filename))
        os.system('./{0}'.format(filename))
    else:
        logger.logger.info('Master script "{0}" written, but will not run.'.format(filename))

# ========================================================================================================

def write_all_bash_jobs_scripts(master,extn,IDs,dir='jobScripts',echo=True,prefix='',pad_length=5, slurm_kwargs={}):
    
    """Write all the bash job scripts for a given set of job IDs.
        
        Arguments:
        ----------
        master : class ``file``
        Master script to which to write contents.
        extn : str
        Extension to append to this job script (e.g. date & time).
        IDs : str
        Comma-separated list of job IDs
        dir : str, optional
        Directory to write this script into.
        echo : bool, optional
        Echo what this job script does for the user?
        prefix : str, optional
        Additional prefix to place on the beginning of these script names.
        pad_length : int, optional
        Length to pad the SLURM sacct output columns.
        slurm_kwargs : list, optional
        Parameters parsed from [slurm] section of config."""
    
    #Add time as extn to this pipeline run, to give unique filenames
    killScript = prefix + 'killJobs'
    summaryScript = prefix + 'summary'
    errorScript = prefix + 'findErrors'
    timingScript = prefix + 'displayTimes'
    cleanupScript = prefix + 'cleanup'
    
    #Write each job script - kill script, summary script, and error script
    write_bash_job_script(master, killScript, extn, 'echo scancel ${0}'.format(IDs), 'kill all the jobs', dir=dir, echo=echo)
    do = """echo sacct -j ${0} --units=G -o "JobID%-15,JobName%-{1},Partition,Elapsed,NNodes%6,NTasks%6,NCPUS%5,MaxDiskRead,MaxDiskWrite,NodeList%20,TotalCPU,CPUTime,MaxRSS,State,ExitCode" """.format(IDs,15+pad_length)
    write_bash_job_script(master, summaryScript, extn, do, 'view the progress', dir=dir, echo=echo)
    do = """echo "for ID in {$%s,}; do ls %s/*\$ID*; cat %s/*\$ID* | grep -i 'severe\|error' | grep -vi 'mpi\|The selected table has zero rows'; done" """ % (IDs,globals.LOG_DIR,globals.LOG_DIR)
    write_bash_job_script(master, errorScript, extn, do, 'find errors \(after pipeline has run\)', dir=dir, echo=echo)
    do = """echo "for ID in {$%s,}; do ls %s/*\$ID*; cat %s/*\$ID* | grep INFO | head -n 1 | cut -d 'I' -f1; cat %s/*\$ID* | grep INFO | tail -n 1 | cut -d 'I' -f1; done" """ % (IDs,globals.LOG_DIR,globals.LOG_DIR,globals.LOG_DIR)
    write_bash_job_script(master, timingScript, extn, do, 'display start and end timestamps \(after pipeline has run\)', dir=dir, echo=echo)
    do = """echo "%s rm -r *ms" """ % srun(slurm_kwargs, qos=True, time=10, mem=1)
    write_bash_job_script(master, cleanupScript, extn, do, 'remove MSs/MMSs from this directory \(after pipeline has run\)', dir=dir, echo=echo)



# ========================================================================================================

def write_bash_job_script(master,filename,extn,do,purpose,dir='jobScripts',echo=True,prefix=''):
    
    """Write bash job script (e.g. jobs summary, kill all jobs, etc).
        
        Arguments:
        ----------
        master : class ``file``
        Master script to which to write contents.
        filename : str
        Filename of this job script.
        extn : str
        Extension to append to this job script (e.g. date & time).
        do : str
        Bash command to run in this job script.
        purpose : str
        Purpose of this script to append as comment.
        dir : str, optional
        Directory to write this script into.
        echo : bool, optional
        Echo what this job script does for the user?
        prefix : str, optional
        Additional prefix to place on the beginning of the script, called from the top level directory (instead of SPW directories)."""
    
    fname = '{0}/{1}{2}'.format(dir,filename,extn)
    do2 = ' ./{0}/{1}{2}{3} \"'.format(dir,prefix,filename,extn) if prefix != '' else ' '
    master.write('\n#Create {0}.sh file, make executable and symlink to current version\n'.format(filename))
    master.write('echo "#!/bin/bash" > {0}\n'.format(fname))
    master.write('{0}{1}>> {2}\n'.format(do,do2,fname))
    master.write('chmod u+x {0}\n'.format(fname))
    master.write('ln -f -s {0} {1}.sh\n'.format(fname,filename))
    if echo:
        master.write('echo Run ./{0}.sh to {1}.\n'.format(filename,purpose))

# ========================================================================================================

