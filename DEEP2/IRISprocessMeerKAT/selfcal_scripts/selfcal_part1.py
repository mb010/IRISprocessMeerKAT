#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import os
sys.path.append(os.getcwd())

from utils import config_parser
from utils.config_parser import validate_args as va
from utils import bookkeeping
#import config_parser
#from config_parser import validate_args as va
#import bookkeeping

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def selfcal_part1(vis, nloops, restart_no, cell, robust, imsize, wprojplanes, niter, threshold,
        multiscale, nterms, gridder, deconvolver, solint, calmode, atrous, loop, refant):

    visbase = os.path.split(vis)[1] # Get only vis name, not entire path
    ## visbase = '1538942495_sdp_l0.J0220-0449.mms' ?
    basename = visbase.replace('.ms', '') + '_im_%d' # Images will be produced in $CWD
    ## basename = '1538942495_sdp_l0.J0220-0449.mms_im_%d'
    imagename = basename % (loop + restart_no)
    ## basename = '1538942495_sdp_l0.J0220-0449.mms_im_0'
    pixmask = basename % (loop + restart_no) + ".pixmask"
    caltable = vis.replace('.ms', '') + '.gcal%d' % (loop + restart_no - 1)
    all_caltables = sorted(glob.glob('*.gcal?'))

    if loop > 0 and not os.path.exists(caltable):
        logger.error("Calibration table {0} doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(caltable,loop))
        sys.exit(1)
    else:
        if loop == 0 and not os.path.exists(pixmask):
            pixmask = ''
            imagename += '_nomask'
        elif 0 < loop <= (nloops):
                applycal(vis=vis, selectdata=False, gaintable=all_caltables, parang=False, interp='linear,linearflag')

                flagdata(vis=vis, mode='rflag', datacolumn='RESIDUAL', field='', timecutoff=5.0,
                    freqcutoff=5.0, timefit='line', freqfit='line', flagdimension='freqtime',
                    extendflags=False, timedevscale=3.0, freqdevscale=3.0, spectralmax=500,
                    extendpols=False, growaround=False, flagneartime=False, flagnearfreq=False,
                    action='apply', flagbackup=True, overwrite=True, writeflags=True)

        tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize[loop], cell=cell[loop], stokes='I', gridder=gridder[loop],
            wprojplanes = wprojplanes[loop], deconvolver = deconvolver[loop], restoration=True,
            weighting='briggs', robust = robust[loop], niter=niter[loop], scales=multiscale[loop],
            threshold=threshold[loop], nterms=nterms[loop],
            savemodel='none', pblimit=-1, mask=pixmask, parallel = True)


if __name__ == '__main__':

    args, params = bookkeeping.get_selfcal_params() 
    # Check if this works with current set up.
    selfcal_part1(**params)


"""
See the dictionary entries in default_config.txt to help understand what this file is meant to do and how it is meant to be called
(and to see if other files also need to be called).
Q: Where does 'loop' come from? Need to check bookkeeping.get_selfcal_params() call.
A: utils.bookkeeping.py > "if 'loop' not in params: params['loop']=0"
"""

"""
Things I know I need:
-vis (.ms final format)
-caltables (*.gcal final format)

Function calls (through params)

vis = 'data/1538856059_sdp_l0.ms'
loop = 0
nloops = 4
restart_no = 0
cell = '2arcsec'
robust = -0.5
imsize = 4096
wprojplanes = 128
niter = [8000, 11000, 14000, 15000, 200000]
threshold = [100e-6, 50e-6, 20e-6, 10e-6, 4e-6] # In units of Jy
multiscale = []
nterms = 2                        # Number of taylor terms
gridder = 'wproject'
deconvolver = 'mtmfs'
solint = ['10min','5min','2min','1min']
calmode = 'p'
atrous = False                     # Source find for diffuse emission (see PyBDSF docs)
refant = 'm059'                   # Reference antenna name / number
"""