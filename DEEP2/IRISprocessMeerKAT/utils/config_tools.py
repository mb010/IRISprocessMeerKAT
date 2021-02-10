import os,sys
from shutil import copyfile

import globals
import config_parser
import logger
from slurm import get_slurm_dict, srun, write_command

# ========================================================================================================
# ========================================================================================================

def default_config(arg_dict):
    
    """Generate default config file in current directory, pointing to MS, with fields and SLURM parameters set.
        
        Arguments:
        ----------
        arg_dict : dict
        Dictionary of arguments passed into this script, which is inserted into the config file under various sections."""
    
    filename = arg_dict['config']
    MS = arg_dict['MS']
    
    #Copy default config to current location
    copyfile('{0}/{1}'.format(globals.SCRIPT_DIR,globals.CONFIG),filename)
    
    #Add SLURM CL arguments to config file under section [slurm]
    slurm_dict = get_slurm_dict(arg_dict,globals.SLURM_CONFIG_KEYS)
    for key in globals.SLURM_CONFIG_STR_KEYS:
        if key in slurm_dict.keys(): slurm_dict[key] = "'{0}'".format(slurm_dict[key])
    
    #Overwrite CL parameters in config under section [slurm]
    config_parser.overwrite_config(filename, conf_dict=slurm_dict, conf_sec='slurm')

    #Add MS to config file under section [data] and dopol under section [run]
    config_parser.overwrite_config(filename, conf_dict={'vis' : "'{0}'".format(MS)}, conf_sec='data')
    config_parser.overwrite_config(filename, conf_dict={'dopol' : arg_dict['dopol']}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
    
    if not arg_dict['do2GC']:
        config_parser.remove_section(filename, 'selfcal')
        scripts = arg_dict['postcal_scripts']
        i = 0
        while i < len(scripts):
            if 'selfcal' in scripts[i][0] or 'bdsf' in scripts[i][0] or scripts[i][0] == 'make_pixmask.py':
                scripts.pop(i)
                i -= 1
            i += 1
        
        config_parser.overwrite_config(filename, conf_dict={'postcal_scripts' : scripts}, conf_sec='slurm')

    if not arg_dict['nofields']:
        #Don't call srun if option --local used
        if arg_dict['local']:
            mpi_wrapper = ''
        else:
            mpi_wrapper = srun(arg_dict)
        
        #Write and submit srun command to extract fields, and insert them into config file under section [fields]
        params =  '-B -M {MS} -C {config} -N {nodes} -t {ntasks_per_node}'.format(**arg_dict)
        if arg_dict['dopol']:
            params += ' -P'
        if arg_dict['verbose']:
            params += ' -v'
        command = write_command(globals.UTILS_DIR+'/read_ms.py', params, mpi_wrapper=mpi_wrapper, container=arg_dict['container'],logfile=False,casa_script=False,casacore=True)
        logger.logger.info('Extracting field IDs from measurement set "{0}" using CASA.'.format(MS))
        logger.logger.debug('Using the following command:\n\t{0}'.format(command))
        os.system(command)
    else:
        #Skip extraction of field IDs and assume we're not processing multiple SPWs
        logger.logger.info('Skipping extraction of field IDs and assuming nspw=1.')
        config_parser.overwrite_config(filename, conf_dict={'nspw' : 1}, conf_sec='crosscal')

    #If dopol=True, replace second call of xx_yy_* scripts with xy_yx_* scripts
    #Check in config (not CL args), in case read_ms.py forces dopol=False, and assume we only want to set this for 'scripts'
    dopol = config_parser.get_key(filename, 'run', 'dopol')
    if dopol:
        count = 0
        for ind, ss in enumerate(arg_dict['scripts']):
            if ss[0] == 'xx_yy_solve.py' or ss[0] == 'xx_yy_apply.py':
                count += 1
            
            if count > 2:
                if ss[0] == 'xx_yy_solve.py':
                    arg_dict['scripts'][ind] = ('xy_yx_solve.py',arg_dict['scripts'][ind][1],arg_dict['scripts'][ind][2])
                if ss[0] == 'xx_yy_apply.py':
                    arg_dict['scripts'][ind] = ('xy_yx_apply.py',arg_dict['scripts'][ind][1],arg_dict['scripts'][ind][2])

        config_parser.overwrite_config(filename, conf_dict={'scripts' : arg_dict['scripts']}, conf_sec='slurm')

    logger.logger.info('Config "{0}" generated.'.format(filename))

    return

# ========================================================================================================

def default_config_iris(arg_dict):
    
    """Generate default config file in current directory, pointing to MS, with fields and SLURM parameters set.
        
        Arguments:
        ----------
        arg_dict : dict
        Dictionary of arguments passed into this script, which is inserted into the config file under various sections."""
    
    filename = arg_dict['config']
    MS = arg_dict['MS']
    
    #Copy default config to current location
    copyfile('{0}/{1}'.format(globals.SCRIPT_DIR,globals.IRISCONFIG),filename)
    
    #Add MS to config file under section [data] and dopol under section [run]
    config_parser.overwrite_config(filename, conf_dict={'vis' : "'{0}'".format(MS)}, conf_sec='data')
    config_parser.overwrite_config(filename, conf_dict={'dopol' : arg_dict['dopol']}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
    
    if not arg_dict['do2GC']:
        config_parser.remove_section(filename, 'selfcal')
        scripts = arg_dict['postcal_scripts']
        i = 0
        while i < len(scripts):
            if 'selfcal' in scripts[i][0] or 'bdsf' in scripts[i][0] or scripts[i][0] == 'make_pixmask.py':
                scripts.pop(i)
                i -= 1
            i += 1
        
        config_parser.overwrite_config(filename, conf_dict={'postcal_scripts' : scripts}, conf_sec='iris')

    if not arg_dict['nofields']:
        logger.logger.info('Field extraction not available for IRIS.')
    
    #Skip extraction of field IDs and assume we're not processing multiple SPWs
    logger.logger.info('Skipping extraction of field IDs')
    #config_parser.overwrite_config(filename, conf_dict={'nspw' : 1}, conf_sec='crosscal')

    #If dopol=True, replace second call of xx_yy_* scripts with xy_yx_* scripts
    #Check in config (not CL args), in case read_ms.py forces dopol=False, and assume we only want to set this for 'scripts'
    dopol = config_parser.get_key(filename, 'run', 'dopol')
    if dopol:
        count = 0
        for ind, ss in enumerate(arg_dict['scripts']):
            if ss[0] == 'xx_yy_solve.py' or ss[0] == 'xx_yy_apply.py':
                count += 1
            
            if count > 2:
                if ss[0] == 'xx_yy_solve.py':
                    arg_dict['scripts'][ind] = ('xy_yx_solve.py',arg_dict['scripts'][ind][1],arg_dict['scripts'][ind][2])
                if ss[0] == 'xx_yy_apply.py':
                    arg_dict['scripts'][ind] = ('xy_yx_apply.py',arg_dict['scripts'][ind][1],arg_dict['scripts'][ind][2])

    config_parser.overwrite_config(filename, conf_dict={'scripts' : arg_dict['scripts']}, conf_sec='iris')

    logger.logger.info('Config "{0}" generated.'.format(filename))

    return

# ========================================================================================================

