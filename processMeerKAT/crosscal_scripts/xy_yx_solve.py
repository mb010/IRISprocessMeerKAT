#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import os,sys
sys.path.append(os.getcwd())
import shutil

from utils import config_parser
from utils import bookkeeping
from utils import ms_tools
from utils.config_parser import validate_args as va
from recipes.almapolhelpers import *

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def do_cross_cal(visname, fields, calfiles, referenceant, caldir,
        minbaselines, standard):

    if not os.path.isdir(caldir):
        os.makedirs(caldir)
    elif not os.path.isdir(caldir+'_round1'):
        os.rename(caldir,caldir+'_round1')
        os.makedirs(caldir)

    xyfield = ms_tools.get_xy_field(visname, fields)

    logger.info(" starting antenna-based delay (kcorr)\n -> %s" % calfiles.kcorrfile)
    gaincal(vis=visname, caltable = calfiles.kcorrfile,
            field = fields.kcorrfield, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  gaintype = 'K',
            solint = '10min', combine = 'scan', parang = False, append = False)
    bookkeeping.check_file(calfiles.kcorrfile)

    logger.info(" starting bandpass -> %s" % calfiles.bpassfile)
    bandpass(vis=visname, caltable = calfiles.bpassfile,
            field = fields.bpassfield,
            refant = referenceant, minblperant = minbaselines, solnorm = False,
            solint = 'inf', combine = 'scan', bandtype = 'B', fillgaps = 8,
            gaintable = calfiles.kcorrfile, gainfield = fields.kcorrfield,
            parang = False, append = False)
    bookkeeping.check_file(calfiles.bpassfile)

    logger.info(" starting cross hand delay -> %s" % calfiles.xdelfile)
    gaincal(vis=visname, caltable = calfiles.xdelfile, field = fields.xdelfield,
            refant = referenceant, smodel=[1., 0., 1., 0.],
            solint = 'inf', minblperant = minbaselines, gaintype = 'KCROSS',
            combine = 'scan',
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile],
            gainfield = [fields.kcorrfield, fields.bpassfield])
    bookkeeping.check_file(calfiles.xdelfile)

    base = visname.replace('.ms', '')
    gain1file   = os.path.join(caldir, base+'.g1cal')
    dtempfile   = os.path.join(caldir, base+'.dtempcal')
    xy0ambpfile = os.path.join(caldir, base+'.xyambcal')
    xy0pfile    = os.path.join(caldir, base+'.xycal')
    xpfile      = os.path.join(caldir, base+'.xfcal')

    # Delete output from any previous calibration run
    if os.path.exists(gain1file):
        shutil.rmtree(gain1file)

    if os.path.exists(dtempfile):
        shutil.rmtree(dtempfile)

    if os.path.exists(xy0ambpfile):
        shutil.rmtree(xy0ambpfile)

    if os.path.exists(xy0pfile):
        shutil.rmtree(xy0pfile)

    logger.info(" starting gaincal -> %s" % gain1file)
    gaincal(vis=visname, caltable=gain1file, field=fields.fluxfield,
            refant=referenceant, solint='10min', minblperant=minbaselines,
            solnorm=False, gaintype='G',
            gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.xdelfield], append=False, parang=True)
    bookkeeping.check_file(gain1file)

    gaincal(vis=visname, caltable=gain1file, field=fields.secondaryfield,
            smodel=[1,0,0,0], refant=referenceant, solint='10min',
            minblperant=minbaselines, solnorm=False, gaintype='G',
            gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.xdelfield],
            append=True, parang=True)
    bookkeeping.check_file(gain1file)

    # implied polarization from instrumental response
    logger.info("\n Solve for Q, U from initial gain solution")
    GainQU = qufromgain(gain1file)
    logger.info(GainQU[int(fields.dpolfield)])

    S = [1.0, GainQU[int(fields.dpolfield)][0],
            GainQU[int(fields.dpolfield)][1], 0.0]

    p = np.sqrt(S[1]**2 + S[2]**2)
    logger.info("Model for polarization calibrator S = {0:.4}".format(S))
    logger.info("Fractional polarization = {0:.4}".format(p))

    gaincal(vis=visname, caltable = calfiles.gainfile, field = fields.fluxfield,
            refant = referenceant, solint = '10min', solnorm = False,
            gaintype = 'G', minblperant = minbaselines, combine = '',
            minsnr = 3, calmode = 'ap',
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield,fields.bpassfield,fields.xdelfield],
            parang = True, append = False)
    bookkeeping.check_file(calfiles.gainfile)


    logger.info("\n solution for secondary with parang = true")
    gaincal(vis=visname, caltable = calfiles.gainfile,
            field = fields.secondaryfield, refant = referenceant,
            solint = '10min', solnorm = False,
            gaintype = 'G', minblperant = minbaselines,
            combine = '', smodel = S, minsnr = 3,
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.xdelfield],
            parang = True, append = True)
    bookkeeping.check_file(calfiles.gainfile)


    # if xyfield is not the leakage cal, run a round of gain calibration on xyfield
    if xyfield != fields.dpolfield:
        gaincal(vis=visname, caltable=calfiles.gainfile, field=xyfield,
                refant=referenceant, solint='10min',
                minblperant=minbaselines, solnorm=False, gaintype='G',
                gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                    calfiles.xdelfile],
                gainfield = [fields.kcorrfield, fields.bpassfield,
                    fields.xdelfield],
            append=True, parang=True)
        bookkeeping.check_file(calfiles.gainfile)

    logger.info("\n now re-solve for Q,U from the new gainfile\n -> %s" % calfiles.gainfile)

    Gain2QU = qufromgain(calfiles.gainfile)
    logger.info(Gain2QU[int(fields.dpolfield)])

    logger.info("\n Starting x-y phase calibration\n -> %s" % xy0ambpfile)
    gaincal(vis=visname, caltable = xy0ambpfile, field = fields.dpolfield,
            refant = referenceant, solint = 'inf', combine = 'scan,2.5MHz',
            gaintype = 'XYf+QU', minblperant = minbaselines,
            smodel = [1.,0.,1.,0.], preavg = 200.0,
            gaintable = [calfiles.kcorrfile,calfiles.bpassfile,
                calfiles.gainfile, calfiles.xdelfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.secondaryfield, fields.xdelfield],
            append = False)
    bookkeeping.check_file(xy0ambpfile)

    logger.info("\n Check for x-y phase ambiguity.")
    S = xyamb(xytab=xy0ambpfile, qu=GainQU[int(fields.dpolfield)], xyout = xy0pfile)

    logger.info("starting \'Df+QU\' polcal -> %s"  % calfiles.dpolfile)
    polcal(vis=visname, caltable = dtempfile, field = fields.dpolfield,
            refant = '', solint = 'inf', combine = 'scan',
            poltype = 'Df+QU', smodel = S, preavg= 200.0,
            gaintable = [calfiles.kcorrfile,calfiles.bpassfile,
                calfiles.gainfile, calfiles.xdelfile, xy0pfile],
           gainfield = [fields.kcorrfield, fields.bpassfield,
               fields.secondaryfield, fields.xdelfield, fields.dpolfield],
           append = False)
    bookkeeping.check_file(dtempfile)

    Dgen(dtab=dtempfile, dout=calfiles.dpolfile)

    # Redo X-Y and gaincal solutions, applying leakage
    if os.path.exists(xy0ambpfile):
        shutil.rmtree(xy0ambpfile)

    if os.path.exists(xy0pfile):
        shutil.rmtree(xy0pfile)

    gaincal(vis=visname, caltable = calfiles.gainfile, field = fields.fluxfield,
            refant = referenceant, solint = '10min', solnorm = False,
            gaintype = 'G', minblperant = minbaselines, combine = '',
            minsnr = 3, calmode = 'ap',
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile, calfiles.dpolfile],
            gainfield = [fields.kcorrfield,fields.bpassfield,fields.xdelfield,
                fields.dpolfield],
            parang = True, append = False)
    bookkeeping.check_file(calfiles.gainfile)


    logger.info("\n solution for secondary with parang = true")
    gaincal(vis=visname, caltable = calfiles.gainfile,
            field = fields.secondaryfield, refant = referenceant,
            solint = '10min', solnorm = False,
            gaintype = 'G', minblperant = minbaselines,
            combine = '', smodel = S, minsnr = 3,
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile, calfiles.dpolfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.xdelfield, fields.dpolfield],
            parang = True, append = True)
    bookkeeping.check_file(calfiles.gainfile)

    # if xyfield is not the leakage cal, run a round of gain calibration on xyfield
    if xyfield != fields.dpolfield:
        gaincal(vis=visname, caltable=calfiles.gainfile, field=xyfield,
                refant=referenceant, solint='10min',
                minblperant=minbaselines, solnorm=False, gaintype='G',
                gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                    calfiles.xdelfile, calfiles.dpolfile],
                gainfield = [fields.kcorrfield, fields.bpassfield,
                    fields.xdelfield, fields.dpolfield],
            append=True, parang=True)
        bookkeeping.check_file(calfiles.gainfile)

    logger.info("\n Starting x-y phase calibration\n -> %s" % xy0ambpfile)
    gaincal(vis=visname, caltable = xy0ambpfile, field = fields.dpolfield,
            refant = referenceant, solint = 'inf', combine = 'scan,2.5MHz',
            gaintype = 'XYf+QU', minblperant = minbaselines,
            smodel = [1.,0.,1.,0.], preavg = 200.0,
            gaintable = [calfiles.kcorrfile,calfiles.bpassfile,
                calfiles.gainfile, calfiles.xdelfile, calfiles.dpolfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.secondaryfield, fields.xdelfield, fields.dpolfield],
            append = False)
    bookkeeping.check_file(xy0ambpfile)

    logger.info("\n Check for x-y phase ambiguity.")
    S = xyamb(xytab=xy0ambpfile, qu=GainQU[int(fields.dpolfield)], xyout = xy0pfile)

    # Only run fluxscale if bootstrapping
    if len(fields.gainfields) > 1:
        logger.info(" starting fluxscale -> %s", calfiles.fluxfile)
        fluxscale(vis=visname, caltable = calfiles.gainfile,
                reference = fields.fluxfield, transfer = '',
                fluxtable = calfiles.fluxfile,
                listfile = os.path.join(caldir,'fluxscale_xy_yx.txt'),
                append = False, display=False)
        bookkeeping.check_file(calfiles.fluxfile)


def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')
    minbaselines = va(taskvals, 'crosscal', 'minbaselines', int, default=4)
    standard = va(taskvals, 'crosscal', 'standard', str, default='Perley-Butler 2010')

    do_cross_cal(visname, fields, calfiles, refant, caldir,
            minbaselines, standard)

if __name__ == '__main__':

    bookkeeping.run_script(main)
