#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import os,sys
sys.path.append(os.getcwd())

from utils import config_parser
from utils.config_parser import validate_args as va
from utils import bookkeeping

def do_pre_flag_2(visname, fields):
    clipfluxcal   = [0., 50.]
    clipphasecal  = [0., 50.]
    cliptarget    = [0., 50.]

    flagdata(vis=visname, mode="clip", field=fields.fluxfield,
            clipminmax=clipfluxcal, datacolumn="corrected", clipoutside=True,
            clipzeros=True, extendpols=False, action="apply", flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="clip",
            field=fields.secondaryfield, clipminmax=clipphasecal,
            datacolumn="corrected", clipoutside=True, clipzeros=True,
            extendpols=False, action="apply", flagbackup=True, savepars=False,
            overwrite=True, writeflags=True)

    # After clip, now flag using 'tfcrop' option for flux and phase cal tight
    # flagging
    flagdata(vis=visname, mode="tfcrop", datacolumn="corrected",
            field=fields.gainfields, ntime="scan", timecutoff=6.0,
            freqcutoff=5.0, timefit="line", freqfit="line",
            flagdimension="freqtime", extendflags=False, timedevscale=5.0,
            freqdevscale=5.0, extendpols=False, growaround=False,
            action="apply", flagbackup=True, overwrite=True, writeflags=True)

    # now flag using 'rflag' option  for flux and phase cal tight flagging
    flagdata(vis=visname, mode="rflag", datacolumn="corrected",
            field=fields.gainfields, timecutoff=5.0, freqcutoff=5.0,
            timefit="poly", freqfit="line", flagdimension="freqtime",
            extendflags=False, timedevscale=4.0, freqdevscale=4.0,
            spectralmax=500.0, extendpols=False, growaround=False,
            flagneartime=False, flagnearfreq=False, action="apply",
            flagbackup=True, overwrite=True, writeflags=True)

    ## Now extend the flags (70% more means full flag, change if required)
    flagdata(vis=visname, mode="extend", field=fields.gainfields,
            datacolumn="corrected", clipzeros=True, ntime="scan",
            extendflags=False, extendpols=False, growtime=90.0, growfreq=90.0,
            growaround=False, flagneartime=False, flagnearfreq=False,
            action="apply", flagbackup=True, overwrite=True, writeflags=True)

    # Now flag for target - moderate flagging, more flagging in self-cal cycles
    flagdata(vis=visname, mode="clip", field=fields.targetfield,
            clipminmax=cliptarget, datacolumn="corrected", clipoutside=True,
            clipzeros=True, extendpols=False, action="apply", flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="tfcrop", datacolumn="corrected",
            field=fields.targetfield, ntime="scan", timecutoff=6.0, freqcutoff=5.0,
            timefit="poly", freqfit="line", flagdimension="freqtime",
            extendflags=False, timedevscale=5.0, freqdevscale=5.0,
            extendpols=False, growaround=False, action="apply", flagbackup=True,
            overwrite=True, writeflags=True)

    # now flag using 'rflag' option
    flagdata(vis=visname, mode="rflag", datacolumn="corrected",
            field=fields.targetfield, timecutoff=5.0, freqcutoff=5.0, timefit="poly",
            freqfit="poly", flagdimension="freqtime", extendflags=False,
            timedevscale=5.0, freqdevscale=5.0, spectralmax=500.0,
            extendpols=False, growaround=False, flagneartime=False,
            flagnearfreq=False, action="apply", flagbackup=True, overwrite=True,
            writeflags=True)

    # Now summary
    flagdata(vis=visname, mode="summary", datacolumn="corrected",
            extendflags=True, name=visname + 'summary.split', action="apply",
            flagbackup=True, overwrite=True, writeflags=True)

def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    do_pre_flag_2(visname, fields)

if __name__ == '__main__':

    bookkeeping.run_script(main)
