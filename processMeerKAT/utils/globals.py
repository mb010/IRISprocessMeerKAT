import os, sys

__version__ = '1.1'

#Set global limits for current ilifu cluster configuration
TOTAL_NODES_LIMIT = 79
CPUS_PER_NODE_LIMIT = 32
NTASKS_PER_NODE_LIMIT = CPUS_PER_NODE_LIMIT
MEM_PER_NODE_GB_LIMIT = 236 #241664 MB
MEM_PER_NODE_GB_LIMIT_HIGHMEM = 482 #493568 MB

#Set global values for paths and file names
#THIS_PROG = __file__
#SCRIPT_DIR = os.path.dirname(THIS_PROG)
SCRIPT_DIR = '/Users/annascaife/SRC/GITHUB/iris_pipelines/processMeerKAT/'
THIS_PROG = 'IRISprocessMeerKAT'
LOG_DIR = 'logs'
CALIB_SCRIPTS_DIR = 'crosscal_scripts'
AUX_SCRIPTS_DIR = 'aux_scripts'
SELFCAL_SCRIPTS_DIR = 'selfcal_scripts'
UTILS_DIR = 'utils'
CONFIG = 'default_config.txt'
TMP_CONFIG = '.config.tmp'
MASTER_SCRIPT = 'submit_pipeline.sh'

#Set global values for field, crosscal and SLURM arguments copied to config file, and some of their default values
FIELDS_CONFIG_KEYS = ['fluxfield','bpassfield','phasecalfield','targetfields','extrafields']
CROSSCAL_CONFIG_KEYS = ['minbaselines','chanbin','width','timeavg','createmms','keepmms','spw','nspw','calcrefant','refant','standard','badants','badfreqranges']
SELFCAL_CONFIG_KEYS = ['nloops','restart_no','cell','robust','imsize','wprojplanes','niter','threshold','multiscale','nterms','gridder','deconvolver','solint','calmode','atrous']
SLURM_CONFIG_STR_KEYS = ['container','mpi_wrapper','partition','time','name','dependencies','exclude','account','reservation']
SLURM_CONFIG_KEYS = ['nodes','ntasks_per_node','mem','plane','submit','precal_scripts','postcal_scripts','scripts','verbose'] + SLURM_CONFIG_STR_KEYS
CONTAINER = '/idia/software/containers/casa-stable-5.6.2-2.simg'
MPI_WRAPPER = '/idia/software/pipelines/casa-pipeline-release-5.6.1-8.el7/bin/mpicasa'
PRECAL_SCRIPTS = [('calc_refant.py',False,''),('partition.py',True,'')] #Scripts run before calibration at top level directory when nspw > 1
POSTCAL_SCRIPTS = [('concat.py',False,''),('plotcal_spw.py', False, ''),('selfcal_part1.py',True,''),('selfcal_part2.py',False,''),('run_bdsf.py', False, ''),('make_pixmask.py', False, '')] #Scripts run after calibration at top level directory when nspw > 1
SCRIPTS = [ ('validate_input.py',False,''),
           ('flag_round_1.py',True,''),
           ('calc_refant.py',False,''),
           ('setjy.py',True,''),
           ('xx_yy_solve.py',False,''),
           ('xx_yy_apply.py',True,''),
           ('flag_round_2.py',True,''),
           ('xx_yy_solve.py',False,''),
           ('xx_yy_apply.py',True,''),
           ('split.py',True,''),
           ('quick_tclean.py',True,''),
           ('plot_solutions.py',False,'')]


IRISCONFIG = 'default_config_iris.txt'
IRIS_CONFIG_KEYS = ['tags','site','platform','outputse','container','outpath','name','verbose','precal_scripts','postcal_scripts','scripts','verbose']
SCRIPTNAME = 'run_process_meerkat.sh'
JDLNAME = 'run_process_meerkat.jdl'
