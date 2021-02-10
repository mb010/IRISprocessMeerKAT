import os,sys
from datetime import datetime
import re
import argparse
import ConfigParser
import ast
from shutil import copyfile

import logger
import globals
import config_parser
from paths import *
import bookkeeping

# ========================================================================================================
# ========================================================================================================

def parse_args():
    
    """Parse arguments into this script.
        
        Returns:
        --------
        args : class ``argparse.ArgumentParser``
        Known and validated arguments."""
    
    def parse_scripts(val):
        
        """Format individual arguments passed into a list for [ -S --scripts] argument, including paths and boolean values.
            
            Arguments/Returns:
            ------------------
            val : bool or str
            Path to script or container, or boolean representing whether that script is threadsafe (for MPI)."""
        
        if val.lower() in ('true','false'):
            return (val.lower() == 'true')
        else:
            return check_path(val)

    parser = argparse.ArgumentParser(prog=globals.THIS_PROG,description='Process MeerKAT data via CASA measurement set. Version: {0}'.format(globals.__version__))

    parser.add_argument("-M","--MS",metavar="path", required=False, type=str, help="Path to measurement set.")
    parser.add_argument("-C","--config",metavar="path", default=globals.CONFIG, required=False, type=str, help="Relative (not absolute) path to config file.")
    parser.add_argument("-N","--nodes",metavar="num", required=False, type=int, default=1,
                        help="Use this number of nodes [default: 1; max: {0}].".format(globals.TOTAL_NODES_LIMIT))
    parser.add_argument("-t","--ntasks-per-node", metavar="num", required=False, type=int, default=8,
                        help="Use this number of tasks (per node) [default: 16; max: {0}].".format(globals.NTASKS_PER_NODE_LIMIT))
    parser.add_argument("-D","--plane", metavar="num", required=False, type=int, default=1,
                        help="Distribute tasks of this block size before moving onto next node [default: 1; max: ntasks-per-node].")
    parser.add_argument("-m","--mem", metavar="num", required=False, type=int, default=globals.MEM_PER_NODE_GB_LIMIT,
                        help="Use this many GB of memory (per node) for threadsafe scripts [default: {0}; max: {0}].".format(globals.MEM_PER_NODE_GB_LIMIT))
    parser.add_argument("-p","--partition", metavar="name", required=False, type=str, default="Main", help="SLURM partition to use [default: 'Main'].")
    parser.add_argument("-T","--time", metavar="time", required=False, type=str, default="12:00:00", help="Time limit to use for all jobs, in the form d-hh:mm:ss [default: '12:00:00'].")
    parser.add_argument("-S","--scripts", action='append', nargs=3, metavar=('script','threadsafe','container'), required=False, type=parse_scripts, default=globals.SCRIPTS,
                        help="Run pipeline with these scripts, in this order, using these containers (3rd value - empty string to default to [-c --container]). Is it threadsafe (2nd value)?")
    parser.add_argument("-b","--precal_scripts", action='append', nargs=3, metavar=('script','threadsafe','container'), required=False, type=parse_scripts, default=globals.PRECAL_SCRIPTS, help="Same as [-S --scripts], but run before calibration.")
    parser.add_argument("-a","--postcal_scripts", action='append', nargs=3, metavar=('script','threadsafe','container'), required=False, type=parse_scripts, default=globals.POSTCAL_SCRIPTS, help="Same as [-S --scripts], but run after calibration.")
    parser.add_argument("-w","--mpi_wrapper", metavar="path", required=False, type=str, default=globals.MPI_WRAPPER,
                        help="Use this mpi wrapper when calling threadsafe scripts [default: '{0}'].".format(globals.MPI_WRAPPER))
    parser.add_argument("-c","--container", metavar="path", required=False, type=str, default=globals.CONTAINER, help="Use this container when calling scripts [default: '{0}'].".format(globals.CONTAINER))
    parser.add_argument("-n","--name", metavar="unique", required=False, type=str, default='', help="Unique name to give this pipeline run (e.g. 'run1_'), appended to the start of all job names. [default: ''].")
    parser.add_argument("-d","--dependencies", metavar="list", required=False, type=str, default='', help="Comma-separated list (without spaces) of SLURM job dependencies (only used when nspw=1). [default: ''].")
    parser.add_argument("-e","--exclude", metavar="nodes", required=False, type=str, default='', help="SLURM worker nodes to exclude [default: ''].")
    parser.add_argument("-A","--account", metavar="group", required=False, type=str, default='b03-idia-ag', help="SLURM accounting group to use (e.g. 'b05-pipelines-ag' - check 'sacctmgr show user $(whoami) -s format=account%%30') [default: 'b03-idia-ag'].")
    parser.add_argument("-r","--reservation", metavar="name", required=False, type=str, default='', help="SLURM reservation to use. [default: ''].")

    parser.add_argument("-l","--local", action="store_true", required=False, default=False, help="Build config file locally (i.e. without calling srun) [default: False].")
    parser.add_argument("-s","--submit", action="store_true", required=False, default=False, help="Submit jobs immediately to SLURM queue [default: False].")
    parser.add_argument("-v","--verbose", action="store_true", required=False, default=False, help="Verbose output? [default: False].")
    parser.add_argument("-q","--quiet", action="store_true", required=False, default=False, help="Activate quiet mode, with suppressed output [default: False].")
    parser.add_argument("-P","--dopol", action="store_true", required=False, default=False, help="Perform polarization calibration in the pipeline [default: False].")
    parser.add_argument("-2","--do2GC", action="store_true", required=False, default=False, help="Perform (2GC) self-calibration in the pipeline [default: False].")
    parser.add_argument("-x","--nofields", action="store_true", required=False, default=False, help="Do not read the input MS to extract field IDs [default: False].")
    parser.add_argument("-I","--iris", action="store_true", required=False, default=False, help="Create pipeline for IRIS rather than Ilifu.")

    #add mutually exclusive group - don't want to build config, run pipeline, or display version at same time
    run_args = parser.add_mutually_exclusive_group(required=True)
    run_args.add_argument("-B","--build", action="store_true", required=False, default=False, help="Build config file using input MS.")
    run_args.add_argument("-R","--run", action="store_true", required=False, default=False, help="Run pipeline with input config file.")
    run_args.add_argument("-V","--version", action="store_true", required=False, default=False, help="Display the version of this pipeline and quit.")
    run_args.add_argument("-L","--license", action="store_true", required=False, default=False, help="Display this program's license and quit.")
    
    args, unknown = parser.parse_known_args()
    
    if len(unknown) > 0:
        parser.error('Unknown input argument(s) present - {0}'.format(unknown))
    
    if args.run:
        if args.config is None:
            parser.error("You must input a config file [--config] to run the pipeline.")
        if not os.path.exists(args.config):
            parser.error("Input config file '{0}' not found. Please set [-C --config] or write a new one with [-B --build].".format(args.config))

    #if user inputs a list a scripts, remove the default list
    if len(args.scripts) > len(globals.SCRIPTS):
        [args.scripts.pop(0) for i in range(len(globals.SCRIPTS))]
        if len(args.precal_scripts) > len(globals.PRECAL_SCRIPTS):
            [args.precal_scripts.pop(0) for i in range(len(globals.PRECAL_SCRIPTS))]
    if len(args.postcal_scripts) > len(globals.POSTCAL_SCRIPTS):
        [args.postcal_scripts.pop(0) for i in range(len(globals.POSTCAL_SCRIPTS))]
    
    #validate arguments before returning them
    validate_args(vars(args),args.config,parser=parser)
    
    return args


# ========================================================================================================

def format_args(config,submit,quiet,dependencies):
    
    """Format (and validate) arguments from config file, to be passed into write_jobs() function.
        
        Arguments:
        ----------
        config : str
        Path to config file.
        submit : bool
        Allow user to force submitting to queue immediately.
        quiet : bool
        Activate quiet mode, with suppressed output?
        dependencies : str
        Comma-separated list of SLURM job dependencies.
        selfcal : bool
        Is selfcal being performed?
        
        Returns:
        --------
        kwargs : dict
        Keyword arguments extracted from [slurm] section of config file, to be passed into write_jobs() function."""
    
    #Ensure all keys exist in these sections
    kwargs = get_config_kwargs(config,'slurm',globals.SLURM_CONFIG_KEYS)
    data_kwargs = get_config_kwargs(config,'data',['vis'])
    get_config_kwargs(config, 'fields', globals.FIELDS_CONFIG_KEYS)
    crosscal_kwargs = get_config_kwargs(config, 'crosscal', globals.CROSSCAL_CONFIG_KEYS)
    
    #Check selfcal params
    if config_parser.has_section(config,'selfcal'):
        selfcal_kwargs = get_config_kwargs(config, 'selfcal', globals.SELFCAL_CONFIG_KEYS)
        bookkeeping.get_selfcal_params()
    
    #Force submit=True if user has requested it during [-R --run]
    if submit:
        kwargs['submit'] = True
    
    #Ensure nspw is integer
    if type(crosscal_kwargs['nspw']) is not int:
        logger.logger.warn("Argument 'nspw'={0} in '{1}' is not an integer. Will set to integer ({2}).".format(crosscal_kwargs['nspw']),config,int(crosscal_kwargs['nspw']))
        crosscal_kwargs['nspw'] = int(crosscal_kwargs['nspw'])
    
    spw = crosscal_kwargs['spw']
    nspw = crosscal_kwargs['nspw']
    mem = int(kwargs['mem'])

    if nspw > 1 and len(kwargs['scripts']) == 0:
        logger.logger.warn('Setting nspw=1, since no "scripts" parameter in "{0}" is empty, so there\'s nothing run inside SPW directories.'.format(config))
        config_parser.overwrite_config(config, conf_dict={'nspw' : 1}, conf_sec='crosscal')
        nspw = 1
    
    #If nspw = 1 and precal or postcal scripts present, overwrite config and reload
    if nspw == 1:
        if len(kwargs['precal_scripts']) > 0 or len(kwargs['postcal_scripts']) > 0:
            logger.logger.warn('Appending "precal_scripts" to beginning of "scripts", and "postcal_script" to end of "scripts", since nspw=1. Overwritting this in "{0}".'.format(config))
            
            #Drop first instance of calc_refant.py from precal scripts in preference for one in scripts (after flag_round_1.py)
            if 'calc_refant.py' in [i[0] for i in kwargs['precal_scripts']] and 'calc_refant.py' in [i[0] for i in kwargs['scripts']]:
                kwargs['precal_scripts'].pop([i[0] for i in kwargs['precal_scripts']].index('calc_refant.py'))
            
            scripts = kwargs['precal_scripts'] + kwargs['scripts'] + kwargs['postcal_scripts']
            config_parser.overwrite_config(config, conf_dict={'scripts' : scripts}, conf_sec='slurm')
            config_parser.overwrite_config(config, conf_dict={'precal_scripts' : []}, conf_sec='slurm')
            config_parser.overwrite_config(config, conf_dict={'postcal_scripts' : []}, conf_sec='slurm')
            kwargs = get_config_kwargs(config,'slurm',globals.SLURM_CONFIG_KEYS)
        else:
            scripts = kwargs['scripts']
    else:
    
        scripts = kwargs['precal_scripts'] + kwargs['postcal_scripts']
    
    kwargs['num_precal_scripts'] = len(kwargs['precal_scripts'])
    
    # Validate kwargs along with MS
    kwargs['MS'] = data_kwargs['vis']
    validate_args(kwargs,config)
    
    #Reformat scripts tuple/list, to extract scripts, threadsafe, and containers as parallel lists
    #Check that path to each script and container exists or is ''
    kwargs['scripts'] = [check_path(i[0]) for i in scripts]
    kwargs['threadsafe'] = [i[1] for i in scripts]
    kwargs['containers'] = [check_path(i[2]) for i in scripts]
    
    if not crosscal_kwargs['createmms']:
        logger.logger.info("You've set 'createmms = False' in '{0}', so forcing 'keepmms = False'. Will use single CPU for every job other than 'quick_tclean.py', if present.".format(config))
        config_parser.overwrite_config(config, conf_dict={'keepmms' : False}, conf_sec='crosscal')
        kwargs['threadsafe'] = [False]*len(scripts)
    
    elif not crosscal_kwargs['keepmms']:
        #Set threadsafe=False for split and postcal scripts (since working with MS not MMS).
        if 'split.py' in kwargs['scripts']:
            kwargs['threadsafe'][kwargs['scripts'].index('split.py')] = False
        if nspw != 1:
            kwargs['threadsafe'][kwargs['num_precal_scripts']:] = [False]*len(kwargs['postcal_scripts'])

    #Set threadsafe=True for quick-tclean, as this uses MPI even for an MS
    if 'quick_tclean.py' in kwargs['scripts']:
        kwargs['threadsafe'][kwargs['scripts'].index('quick_tclean.py')] = True
    
    #Only reduce the memory footprint if we're not using all CPUs on each node
    # if kwargs['ntasks_per_node'] < NTASKS_PER_NODE_LIMIT:
    #     mem = mem // nspw
    
    dopol = config_parser.get_key(config, 'run', 'dopol')
    if not dopol and ('xy_yx_solve.py' in kwargs['scripts'] or 'xy_yx_apply.py' in kwargs['scripts']):
        logger.logger.warn("Cross-hand calibration scripts 'xy_yx_*' found in scripts. Forcing dopol=True in '[run]' section of '{0}'.".format(config))
        config_parser.overwrite_config(config, conf_dict={'dopol' : True}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
    
    includes_partition = any('partition' in script for script in kwargs['scripts'])
    #If single correctly formatted spw, split into nspw directories, and process each spw independently
    if nspw > 1:
        #Write timestamp to this pipeline run
        kwargs['timestamp'] = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        config_parser.overwrite_config(config, conf_dict={'timestamp' : "'{0}'".format(kwargs['timestamp'])}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
        nspw = spw_split(spw, nspw, config, mem, crosscal_kwargs['badfreqranges'],kwargs['MS'],includes_partition, createmms = crosscal_kwargs['createmms'])
        config_parser.overwrite_config(config, conf_dict={'nspw' : "{0}".format(nspw)}, conf_sec='crosscal')
    
    #Pop script to calculate reference antenna if calcrefant=False. Assume it won't be in postcal scripts
    if not crosscal_kwargs['calcrefant']:
        if pop_script(kwargs,'calc_refant.py'):
            kwargs['num_precal_scripts'] -= 1

    #Replace empty containers with default container and remove unwanted kwargs
    for i in range(len(kwargs['containers'])):
        if kwargs['containers'][i] == '':
            kwargs['containers'][i] = kwargs['container']
    kwargs.pop('container')
    kwargs.pop('MS')
    kwargs.pop('precal_scripts')
    kwargs.pop('postcal_scripts')
    kwargs['quiet'] = quiet

    #Force overwrite of dependencies
    if dependencies != '':
        kwargs['dependencies'] = dependencies
    
    if len(kwargs['scripts']) == 0:
        logger.logger.error('Nothing to do. Please insert scripts into "scripts" parameter in "{0}".'.format(config))
        sys.exit(1)
    
    #If everything up until here has passed, we can copy config file to TMP_CONFIG (in case user runs sbatch manually) and inform user
    logger.logger.debug("Copying '{0}' to '{1}', and using this to run pipeline.".format(config,globals.TMP_CONFIG))
    copyfile(config, globals.TMP_CONFIG)
    if not quiet:
        logger.logger.warn("Changing [slurm] section in your config will have no effect unless you [-R --run] again.")
    
    return kwargs


# ========================================================================================================

def get_config_kwargs(config,section,expected_keys):
    
    """Return kwargs from config section. Check section exists, and that all expected keys are present, otherwise raise KeyError.
        
        Arguments:
        ----------
        config : str
        Path to config file.
        section : str
        Config section from which to extract kwargs.
        expected_keys : list
        List of expected keys.
        
        Returns:
        --------
        kwargs : dict
        Keyword arguments from this config section."""
    
    config_dict = config_parser.parse_config(config)[0]
    
    #Ensure section exists, otherwise raise KeyError
    if section not in config_dict.keys():
        raise KeyError("Config file '{0}' has no section [{1}]. Please insert section or build new config with [-B --build].".format(config,section))

    kwargs = config_dict[section]

    #Check for any unknown keys and display warning
    unknown_keys = list(set(kwargs) - set(expected_keys))
    if len(unknown_keys) > 0:
        logger.logger.warn("Unknown keys {0} present in section [{1}] in '{2}'.".format(unknown_keys,section,config))

    #Check that expected keys are present, otherwise raise KeyError
    missing_keys = list(set(expected_keys) - set(kwargs))
    if len(missing_keys) > 0:
        raise KeyError("Keys {0} missing from section [{1}] in '{2}'. Please add these keywords to '{2}', or else run [-B --build] step again.".format(missing_keys,section,config))
    
    return kwargs


# ========================================================================================================

def validate_args(args,config,parser=None):
    
    """Validate arguments, coming from command line or config file. Raise relevant error (parser error or ValueError) if invalid argument found.
        
        Arguments:
        ----------
        args : dict
        Dictionary of slurm arguments from command line or config file.
        config : str
        Path to config file.
        parser : class ``argparse.ArgumentParser``, optional
        If this is input, parser error will be raised."""
    
    if parser is None or args['build']:
        if args['MS'] is None and not args['nofields']:
            msg = "You must input an MS [-M --MS] to build the config file."
            raise_error(config, msg, parser)
        
        if args['MS'] not in [None,'None'] and not os.path.isdir(args['MS']):
            msg = "Input MS '{0}' not found.".format(args['MS'])
            raise_error(config, msg, parser)

    if parser is not None and not args['build'] and args['MS']:
        msg = "Only input an MS [-M --MS] during [-B --build] step. Otherwise input is ignored."
        raise_error(config, msg, parser)
    
    if args['ntasks_per_node'] > globals.NTASKS_PER_NODE_LIMIT:
        msg = "The number of tasks per node [-t --ntasks-per-node] must not exceed {0}. You input {1}.".format(globals.NTASKS_PER_NODE_LIMIT,args['ntasks_per_node'])
        raise_error(config, msg, parser)
    
    if args['nodes'] > globals.TOTAL_NODES_LIMIT:
        msg = "The number of nodes [-N --nodes] per node must not exceed {0}. You input {1}.".format(globals.TOTAL_NODES_LIMIT,args['nodes'])
        raise_error(config, msg, parser)
    
    if args['mem'] > globals.MEM_PER_NODE_GB_LIMIT:
        if args['partition'] != 'HighMem':
            msg = "The memory per node [-m --mem] must not exceed {0} (GB). You input {1} (GB).".format(globals.MEM_PER_NODE_GB_LIMIT,args['mem'])
            raise_error(config, msg, parser)
        elif args['mem'] > globals.MEM_PER_NODE_GB_LIMIT_HIGHMEM:
            msg = "The memory per node [-m --mem] must not exceed {0} (GB) when using 'HighMem' partition. You input {1} (GB).".format(globals.MEM_PER_NODE_GB_LIMIT_HIGHMEM,args['mem'])
            raise_error(config, msg, parser)

    if args['plane'] > args['ntasks_per_node']:
        msg = "The value of [-P --plane] cannot be greater than the tasks per node [-t --ntasks-per-node] ({0}). You input {1}.".format(args['ntasks_per_node'],args['plane'])
        raise_error(config, msg, parser)
    
    if args['account'] not in ['b03-idia-ag','b05-pipelines-ag']:
        from platform import node
        if node() == 'slurm-login' or 'slwrk' in node():
            accounts=os.popen("for f in $(sacctmgr show user $(whoami) -s format=account%30 | grep -v 'Account\|--'); do echo -n $f,; done").read()[:-1].split(',')
            if args['account'] not in accounts:
                msg = "Accounting group '{0}' not recognised. Please select one of the following from your groups: {1}.".format(args['account'],accounts)
                raise_error(config, msg, parser)
        else:
            msg = "Accounting group '{0}' not recognised. You're not using a SLURM node, so cannot query your accounts.".format(args['account'])
            raise_error(config, msg, parser)


# ========================================================================================================

def spw_split(spw,nspw,config,mem,badfreqranges,MS,partition,createmms=True,remove=True):
    
    """Split into N SPWs, placing an instance of the pipeline into N directories, each with 1 Nth of the bandwidth.
        
        Arguments:
        ----------
        spw : str
        spw parameter from config.
        nspw : int
        Number of spectral windows to split into.
        config : str
        Path to config file.
        mem : int
        Memory in GB to use per instance.
        badfreqranges : list
        List of bad frequency ranges in MHz.
        MS : str
        Path to CASA Measurement Set.
        partition : bool
        Does this run include the partition step?
        createmms : bool
        Create MMS as output?
        remove : bool, optional
        Remove SPWs completely encompassed by bad frequency ranges?
        
        Returns:
        --------
        nspw : int
        New nspw, potentially a lower value than input (if any SPWs completely encompassed by badfreqranges)."""
    
    if get_spw_bounds(spw) != None:
        #Write nspw frequency ranges
        low,high,unit,func = get_spw_bounds(spw)
        interval=func((high-low)/float(nspw))
        lo=linspace(low,high-interval,nspw)
        hi=linspace(low+interval,high,nspw)
        SPWs=[]
        
        #Remove SPWs entirely encompassed by bad frequency ranges (only for MHz unit)
        for i in range(len(lo)):
            SPWs.append('0:{0}~{1}{2}'.format(func(lo[i]),func(hi[i]),unit))

    elif ',' in spw:
        SPWs = spw.split(',')
        unit = get_spw_bounds(SPWs[0])[2]
        if len(SPWs) != nspw:
            logger.logger.error("nspw ({0}) not equal to number of separate SPWs ({1} in '{2}') from '{3}'. Setting to nspw={1}.".format(nspw,len(SPWs),spw,config))
            nspw = len(SPWs)
    else:
        logger.logger.error("Can't split into {0} SPWs using SPW format '{1}'. Using nspw=1 in '{2}'.".format(nspw,spw,config))
        return 1
    
    #Remove any SPWs completely encompassed by bad frequency ranges
    i=0
    while i < nspw:
        badfreq = False
        low,high = get_spw_bounds(SPWs[i])[0:2]
        if unit == 'MHz' and remove:
            for freq in badfreqranges:
                bad_low,bad_high = get_spw_bounds('0:{0}'.format(freq))[0:2]
                if low >= bad_low and high <= bad_high:
                    logger.logger.info("Won't process spw '0:{0}~{1}{2}', since it's completely encompassed by bad frequency range '{3}'.".format(low,high,unit,freq))
                    badfreq = True
                    break
        if badfreq:
            SPWs.pop(i)
            i -= 1
            nspw -= 1
        i += 1

    #Overwrite config with new SPWs
    config_parser.overwrite_config(config, conf_dict={'spw' : "'{0}'".format(','.join(SPWs))}, conf_sec='crosscal')

    #Create each spw as directory and place config in there
    logger.logger.info("Making {0} directories for SPWs ({1}) and copying '{2}' to each of them.".format(nspw,SPWs,config))
    for spw in SPWs:
        spw_config = '{0}/{1}'.format(spw.replace('0:',''),config)
        if not os.path.exists(spw.replace('0:','')):
            os.mkdir(spw.replace('0:',''))
        copyfile(config, spw_config)
        config_parser.overwrite_config(spw_config, conf_dict={'spw' : "'{0}'".format(spw)}, conf_sec='crosscal')
        config_parser.overwrite_config(spw_config, conf_dict={'nspw' : 1}, conf_sec='crosscal')
        config_parser.overwrite_config(spw_config, conf_dict={'mem' : mem}, conf_sec='slurm')
        config_parser.overwrite_config(spw_config, conf_dict={'calcrefant' : False}, conf_sec='crosscal')
        config_parser.overwrite_config(spw_config, conf_dict={'precal_scripts' : []}, conf_sec='slurm')
        config_parser.overwrite_config(spw_config, conf_dict={'postcal_scripts' : []}, conf_sec='slurm')
        #Look 1 directory up when using relative path
        if MS[0] != '/':
            config_parser.overwrite_config(spw_config, conf_dict={'vis' : "'../{0}'".format(MS)}, conf_sec='data')
        elif not partition:
            basename, ext = os.path.splitext(MS.rstrip('/ '))
            filebase = os.path.split(basename)[1]
            extn = 'mms' if createmms else 'ms'
            vis = '{0}.{1}.{2}'.format(filebase,spw.replace('0:',''),extn)
            logger.warn("Since script with 'partition' in its name isn't present in '{0}', assuming partition has already been done, and setting vis='{1}' in '{2}'. If '{1}' doesn't exist, please update '{2}', as the pipeline will not launch successfully.".format(config,vis,spw_config))
            orig_vis = config_parser.get_key(spw_config, 'data', 'vis')
            config_parser.overwrite_config(spw_config, conf_dict={'orig_vis' : "'{0}'".format(orig_vis)}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
            config_parser.overwrite_config(spw_config, conf_dict={'vis' : "'{0}'".format(vis)}, conf_sec='data')

    return nspw

# ========================================================================================================

def format_args_iris(config,submit,quiet,dependencies):
    
    """Format (and validate) arguments from config file, to be passed into write_jobs() function.
        
        Arguments:
        ----------
        config : str
        Path to config file.
        submit : bool
        Allow user to force submitting to queue immediately.
        quiet : bool
        Activate quiet mode, with suppressed output?
        dependencies : str
        Comma-separated list of SLURM job dependencies.
        selfcal : bool
        Is selfcal being performed?
        
        Returns:
        --------
        kwargs : dict
        Keyword arguments extracted from [iris] section of config file, to be passed into write_jobs() function."""
    
    #Ensure all keys exist in these sections
    kwargs = get_config_kwargs(config,'iris',globals.IRIS_CONFIG_KEYS)
    data_kwargs = get_config_kwargs(config,'data',['vis'])
    get_config_kwargs(config, 'fields', globals.FIELDS_CONFIG_KEYS)
    crosscal_kwargs = get_config_kwargs(config, 'crosscal', globals.CROSSCAL_CONFIG_KEYS)
    
    #Check selfcal params
    if config_parser.has_section(config,'selfcal'):
        selfcal_kwargs = get_config_kwargs(config, 'selfcal', globals.SELFCAL_CONFIG_KEYS)
        bookkeeping.get_selfcal_params()
    
    #Force submit=True if user has requested it during [-R --run]
    if submit:
        kwargs['submit'] = True
    
    #Ensure nspw is integer
    if type(crosscal_kwargs['nspw']) is not int:
        logger.logger.warn("Argument 'nspw'={0} in '{1}' is not an integer. Will set to integer ({2}).".format(crosscal_kwargs['nspw']),config,int(crosscal_kwargs['nspw']))
        crosscal_kwargs['nspw'] = int(crosscal_kwargs['nspw'])
    
    spw = crosscal_kwargs['spw']
    nspw = crosscal_kwargs['nspw']
    #mem = int(kwargs['mem'])

    if nspw > 1 and len(kwargs['scripts']) == 0:
        logger.logger.warn('Setting nspw=1, since no "scripts" parameter in "{0}" is empty, so there\'s nothing run inside SPW directories.'.format(config))
        config_parser.overwrite_config(config, conf_dict={'nspw' : 1}, conf_sec='crosscal')
        nspw = 1
    
    #If nspw = 1 and precal or postcal scripts present, overwrite config and reload
    if nspw == 1:
        if len(kwargs['precal_scripts']) > 0 or len(kwargs['postcal_scripts']) > 0:
            logger.logger.warn('Appending "precal_scripts" to beginning of "scripts", and "postcal_script" to end of "scripts", since nspw=1. Overwritting this in "{0}".'.format(config))
            
            #Drop first instance of calc_refant.py from precal scripts in preference for one in scripts (after flag_round_1.py)
            if 'calc_refant.py' in [i[0] for i in kwargs['precal_scripts']] and 'calc_refant.py' in [i[0] for i in kwargs['scripts']]:
                kwargs['precal_scripts'].pop([i[0] for i in kwargs['precal_scripts']].index('calc_refant.py'))
            
            scripts = kwargs['precal_scripts'] + kwargs['scripts'] + kwargs['postcal_scripts']
            config_parser.overwrite_config(config, conf_dict={'scripts' : scripts}, conf_sec='iris')
            config_parser.overwrite_config(config, conf_dict={'precal_scripts' : []}, conf_sec='iris')
            config_parser.overwrite_config(config, conf_dict={'postcal_scripts' : []}, conf_sec='iris')
            kwargs = get_config_kwargs(config,'iris',globals.IRIS_CONFIG_KEYS)
        else:
            scripts = kwargs['scripts']
    else:
        
        scripts = kwargs['precal_scripts'] + kwargs['postcal_scripts']

    kwargs['num_precal_scripts'] = len(kwargs['precal_scripts'])

    # Validate kwargs along with MS
    kwargs['MS'] = data_kwargs['vis']
    validate_args_iris(kwargs,config)
    
    #Reformat scripts tuple/list, to extract scripts, threadsafe, and containers as parallel lists
    #Check that path to each script and container exists or is ''
    kwargs['scripts'] = [check_path(i[0]) for i in scripts]
    kwargs['threadsafe'] = [i[1] for i in scripts]
    kwargs['containers'] = [check_path(i[2]) for i in scripts]
    
    if not crosscal_kwargs['createmms']:
        logger.logger.info("You've set 'createmms = False' in '{0}', so forcing 'keepmms = False'. Will use single CPU for every job other than 'quick_tclean.py', if present.".format(config))
        config_parser.overwrite_config(config, conf_dict={'keepmms' : False}, conf_sec='crosscal')
        kwargs['threadsafe'] = [False]*len(scripts)

    elif not crosscal_kwargs['keepmms']:
        #Set threadsafe=False for split and postcal scripts (since working with MS not MMS).
        if 'split.py' in kwargs['scripts']:
            kwargs['threadsafe'][kwargs['scripts'].index('split.py')] = False
            if nspw != 1:
                kwargs['threadsafe'][kwargs['num_precal_scripts']:] = [False]*len(kwargs['postcal_scripts'])

    #Set threadsafe=True for quick-tclean, as this uses MPI even for an MS
    if 'quick_tclean.py' in kwargs['scripts']:
        kwargs['threadsafe'][kwargs['scripts'].index('quick_tclean.py')] = True
    
    #Only reduce the memory footprint if we're not using all CPUs on each node
    # if kwargs['ntasks_per_node'] < NTASKS_PER_NODE_LIMIT:
    #     mem = mem // nspw
    
    dopol = config_parser.get_key(config, 'run', 'dopol')
    if not dopol and ('xy_yx_solve.py' in kwargs['scripts'] or 'xy_yx_apply.py' in kwargs['scripts']):
        logger.logger.warn("Cross-hand calibration scripts 'xy_yx_*' found in scripts. Forcing dopol=True in '[run]' section of '{0}'.".format(config))
        config_parser.overwrite_config(config, conf_dict={'dopol' : True}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')

    includes_partition = any('partition' in script for script in kwargs['scripts'])
    #If single correctly formatted spw, split into nspw directories, and process each spw independently
    if nspw > 1:
        #Write timestamp to this pipeline run
        kwargs['timestamp'] = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        config_parser.overwrite_config(config, conf_dict={'timestamp' : "'{0}'".format(kwargs['timestamp'])}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
        nspw = spw_split_iris(spw, nspw, config, crosscal_kwargs['badfreqranges'],kwargs['MS'], includes_partition, createmms = crosscal_kwargs['createmms'])
        config_parser.overwrite_config(config, conf_dict={'nspw' : "{0}".format(nspw)}, conf_sec='crosscal')

    #Pop script to calculate reference antenna if calcrefant=False. Assume it won't be in postcal scripts
    if not crosscal_kwargs['calcrefant']:
        if pop_script(kwargs,'calc_refant.py'):
            kwargs['num_precal_scripts'] -= 1

    #Replace empty containers with default container and remove unwanted kwargs
    for i in range(len(kwargs['containers'])):
        if kwargs['containers'][i] == '':
            kwargs['containers'][i] = kwargs['container']
    kwargs.pop('container')
    kwargs.pop('MS')
    kwargs.pop('precal_scripts')
    kwargs.pop('postcal_scripts')
    kwargs['quiet'] = quiet

    #Force overwrite of dependencies
    if dependencies != '':
        kwargs['dependencies'] = dependencies
    
    if len(kwargs['scripts']) == 0:
        logger.logger.error('Nothing to do. Please insert scripts into "scripts" parameter in "{0}".'.format(config))
        sys.exit(1)

    #If everything up until here has passed, we can copy config file to TMP_CONFIG (in case user runs sbatch manually) and inform user
    logger.logger.debug("Copying '{0}' to '{1}', and using this to run pipeline.".format(config,globals.TMP_CONFIG))
    copyfile(config, globals.TMP_CONFIG)
    if not quiet:
        logger.logger.warn("Changing [iris] section in your config will have no effect until you submit your jdl")
    
    return kwargs


# ========================================================================================================

def validate_args_iris(args,config,parser=None):
    
    """Validate arguments, coming from command line or config file. Raise relevant error (parser error or ValueError) if invalid argument found.
        
        Arguments:
        ----------
        args : dict
        Dictionary of slurm arguments from command line or config file.
        config : str
        Path to config file.
        parser : class ``argparse.ArgumentParser``, optional
        If this is input, parser error will be raised."""
    
    if parser is None or args['build']:
        if args['MS'] is None and not args['nofields']:
            msg = "You must input an MS [-M --MS] to build the config file."
            raise_error(config, msg, parser)
        
        if args['MS'] not in [None,'None'] and not os.path.isdir(args['MS']) and not args['MS'][0:3]=="LFN":
            msg = "Input MS '{0}' not found.".format(args['MS'])
            raise_error(config, msg, parser)

    if parser is not None and not args['build'] and args['MS']:
        msg = "Only input an MS [-M --MS] during [-B --build] step. Otherwise input is ignored."
        raise_error(config, msg, parser)


# ========================================================================================================

def spw_split_iris(spw,nspw,config,badfreqranges,MS,partition,createmms=True,remove=True):
    
    """Split into N SPWs, placing an instance of the pipeline into N directories, each with 1 Nth of the bandwidth.
        
        Arguments:
        ----------
        spw : str
        spw parameter from config.
        nspw : int
        Number of spectral windows to split into.
        config : str
        Path to config file.
        mem : int
        Memory in GB to use per instance.
        badfreqranges : list
        List of bad frequency ranges in MHz.
        MS : str
        Path to CASA Measurement Set.
        partition : bool
        Does this run include the partition step?
        createmms : bool
        Create MMS as output?
        remove : bool, optional
        Remove SPWs completely encompassed by bad frequency ranges?
        
        Returns:
        --------
        nspw : int
        New nspw, potentially a lower value than input (if any SPWs completely encompassed by badfreqranges)."""
    
    if get_spw_bounds(spw) != None:
        #Write nspw frequency ranges
        low,high,unit,func = get_spw_bounds(spw)
        interval=func((high-low)/float(nspw))
        lo=linspace(low,high-interval,nspw)
        hi=linspace(low+interval,high,nspw)
        SPWs=[]
        
        #Remove SPWs entirely encompassed by bad frequency ranges (only for MHz unit)
        for i in range(len(lo)):
            SPWs.append('0:{0}~{1}{2}'.format(func(lo[i]),func(hi[i]),unit))

    elif ',' in spw:
        SPWs = spw.split(',')
        unit = get_spw_bounds(SPWs[0])[2]
        if len(SPWs) != nspw:
            logger.logger.error("nspw ({0}) not equal to number of separate SPWs ({1} in '{2}') from '{3}'. Setting to nspw={1}.".format(nspw,len(SPWs),spw,config))
            nspw = len(SPWs)
    else:
        logger.logger.error("Can't split into {0} SPWs using SPW format '{1}'. Using nspw=1 in '{2}'.".format(nspw,spw,config))
        return 1
    
    #Remove any SPWs completely encompassed by bad frequency ranges
    i=0
    while i < nspw:
        badfreq = False
        low,high = get_spw_bounds(SPWs[i])[0:2]
        if unit == 'MHz' and remove:
            for freq in badfreqranges:
                bad_low,bad_high = get_spw_bounds('0:{0}'.format(freq))[0:2]
                if low >= bad_low and high <= bad_high:
                    logger.logger.info("Won't include spw '0:{0}~{1}{2}', since it's completely encompassed by bad frequency range '{3}'.".format(low,high,unit,freq))
                    badfreq = True
                    break
        if badfreq:
            SPWs.pop(i)
            i -= 1
            nspw -= 1
        i += 1

    #Overwrite config with new SPWs
    config_parser.overwrite_config(config, conf_dict={'spw' : "'{0}'".format(','.join(SPWs))}, conf_sec='crosscal')

    spw_config = '{0}_{1}'.format(config[:-4],"calib.txt")
    copyfile(config, spw_config)
    config_parser.overwrite_config(spw_config, conf_dict={'nspw' : nspw}, conf_sec='crosscal')
    config_parser.overwrite_config(spw_config, conf_dict={'calcrefant' : False}, conf_sec='crosscal')
    #config_parser.overwrite_config(spw_config, conf_dict={'precal_scripts' : []}, conf_sec='iris')
    #config_parser.overwrite_config(spw_config, conf_dict={'postcal_scripts' : []}, conf_sec='iris')

    basename, ext = os.path.splitext(MS.rstrip('/ '))
    filebase = os.path.split(basename)[1].rstrip('.ms.tar')
    extn = 'mms' if createmms else 'ms'
    vis = '{0}.{1}.{2}.{3}'.format(filebase,spw.replace('0:',''),extn,'tar')
    orig_vis = config_parser.get_key(spw_config, 'data', 'vis')
    config_parser.overwrite_config(spw_config, conf_dict={'orig_vis' : "'{0}'".format(orig_vis)}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
    config_parser.overwrite_config(spw_config, conf_dict={'vis' : "'{0}'".format(vis)}, conf_sec='data')

    return nspw

# ========================================================================================================

def get_spw_bounds(spw):
    
    """Get upper and lower bounds of spw.
        
        Arguments:
        ----------
        spw : str
        CASA spectral window in MHz.
        
        Returns:
        --------
        low : float
        Lower bound of spw.
        high : float
        Higher bound of spw.
        unit : str
        Unit of spw.
        func : function
        Function to apply to spectral window (i.e. int for SPW channel range, otherwise float)."""
    
    bounds = spw.split(':')[-1].split('~')
    if ',' not in spw and ':' in spw and '~' in spw and len(bounds) == 2 and bounds[1] != '':
        high,unit=re.search(r'(\d+\.*\d*)(\w*)',bounds[1]).groups()
        func = int if unit == '' else float
        low = func(bounds[0])
        high = func(high)
        
        if unit != 'MHz':
            logger.warn('Please use SPW unit "MHz", to ensure the best performance (e.g. not processing entirely flagged frequency ranges).')
        # Can only do when using CASA
        # if unit == '':
        #     msmd.open(MS)
        #     low_MHz = msmd.chanfreqs(0)[low] / 1e6
        #     high_MHz = msmd.chanfreqs(0)[high] / 1e6
        #     msmd.done()
        # else:
        #     low_MHz=qa.convertfreq('{0}{1}'.format(low,unit),'MHz')['value']
        #     high_MHz=qa.convertfreq('{0}{1}'.format(high,unit),'MHz')['value']
    else:
        return None
    
    return low,high,unit,func

# ========================================================================================================

def linspace(lower,upper,length):
    
    """Basically np.linspace, but without needing to import numpy..."""
    
    return [lower + x*(upper-lower)/float(length-1) for x in range(length)]

# ========================================================================================================

def pop_script(kwargs,script):
    
    """Pop script from list of scripts, list of threadsafe tasks, and list of containers.
        
        Arguments:
        ----------
        kwargs :  : dict
        Keyword arguments extracted from [slurm] section of config file, to be passed into write_jobs() function.
        script : str
        Name of script.
        
        Returns:
        --------
        popped : bool
        Was the script popped?"""
    
    popped = False
    if script in kwargs['scripts']:
        index = kwargs['scripts'].index(script)
        kwargs['scripts'].pop(index)
        kwargs['threadsafe'].pop(index)
        kwargs['containers'].pop(index)
        popped = True
    return popped

# ========================================================================================================

def raise_error(config,msg,parser=None):
    
    """Raise error with specified message, either as parser error (when option passed in via command line),
        or ValueError (when option passed in via config file).
        
        Arguments:
        ----------
        config : str
        Path to config file.
        msg : str
        Error message to display.
        parser : class ``argparse.ArgumentParser``, optional
        If this is input, parser error will be raised."""
    
    if parser is None:
        raise ValueError("Bad input found in '{0}' -- {1}".format(config,msg))
    else:
        parser.error(msg)

