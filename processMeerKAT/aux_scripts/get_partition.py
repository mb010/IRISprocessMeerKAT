#!/usr/bin/env python2.7
import os, sys
sys.path.append(os.getcwd())

import numpy as np
import logging
from time import gmtime
logging.Formatter.converter = gmtime

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.DEBUG)

from utils import bookkeeping
from utils import config_parser
from utils.config_parser import validate_args as va

# ========================================================================================================

def main(args,taskvals):
    
    spw = sys.argv[-1]
    
    visname = va(taskvals, "data", "vis", str)
    visname = visname.split('/')[-1]
    mmsfile = visname.replace('ms',spw+'.mms')
    mmsfile = "'{0}'".format(mmsfile)
    
    config_parser.overwrite_config(args['config'], conf_sec='data', conf_dict={'vis':mmsfile})
    logger.info('Updated (vis) in [data] section written to "{0}".'.format(args['config']))
    
    spw = '0:'+spw
    spw = "'{0}'".format(spw)
    config_parser.overwrite_config(args['config'], conf_sec='crosscal', conf_dict={'spw':spw})
    config_parser.overwrite_config(args['config'], conf_sec='crosscal', conf_dict={'nspw':1})
        
    logger.info('Updated (spw,nspw) in [crosscal] section written to "{0}".'.format(args['config']))
    
    return

# ========================================================================================================
# ========================================================================================================

if __name__ == '__main__':
    
    bookkeeping.run_script(main)
