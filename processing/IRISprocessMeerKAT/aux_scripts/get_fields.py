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

# Get access to the msmd module for read_ms.py
import casac
msmd = casac.casac.msmetadata()
tb = casac.casac.table()
me = casac.casac.measures()

# ========================================================================================================
# ========================================================================================================

def read_fields(MS):

    """Extract field numbers from intent, including calibrators for bandpass, flux, phase & amplitude, and the target. Only the
        target allows for multiple field IDs, while all others extract the field with the most scans and put all other IDs as target fields.

        Arguments:
        ----------
        MS : str
        Input measurement set (relative or absolute path).

        Returns:
        --------
        fieldIDs : dict
        fluxfield : int
        Field for total flux calibration.
        bpassfield : int
        Field for bandpass calibration.
        phasecalfield : int
        Field for phase calibration.
        targetfields : int
        Target field."""

    fieldIDs = {}
    extra_fields = []

    # open MS:
    msmd.open(MS)

    #Set default for any missing intent as field for intent CALIBRATE_FLUX
    default = msmd.fieldsforintent('CALIBRATE_FLUX')
    if default.size == 0:
        logger.error('You must have a field with intent "CALIBRATE_FLUX". I found {0} in dataset "{1}".'.format(default.size,MS))
        return fieldIDs

    #Use 'CALIBRATE_PHASE' or if missing, 'CALIBRATE_AMPLI'
    phasecal_intent = 'CALIBRATE_PHASE'
    if phasecal_intent not in msmd.intents():
        phasecal_intent = 'CALIBRATE_AMPLI'

    fieldIDs['fluxfield'] = get_field(MS,'CALIBRATE_FLUX','fluxfield',extra_fields)
    fieldIDs['bpassfield'] = get_field(MS,'CALIBRATE_BANDPASS','bpassfield',extra_fields,default=default)
    fieldIDs['phasecalfield'] = get_field(MS,phasecal_intent,'phasecalfield',extra_fields,default=default)
    fieldIDs['targetfields'] = get_field(MS,'TARGET','targetfields',extra_fields,default=default,multiple=True)

    #Put any extra fields in target fields
    if len(extra_fields) > 0:
        fieldIDs['extrafields'] = "'{0}'".format(','.join([str(extra_fields[i]) for i in range(len(extra_fields))]))

    msmd.done()

    return fieldIDs

# ========================================================================================================

def get_field(MS,intent,fieldname,extra_fields,default=0,multiple=False):

    """Extract field IDs based on intent. When multiple fields are present, if multiple is True, return a
        comma-seperated string, otherwise return a single field string corresponding to the field with the most scans.

        Arguments:
        ----------
        MS : str
        Input measurement set (relative or absolute path).
        intent : str
        Calibration intent.
        fieldname : str
        The name given by the pipeline to the field being extracted (for output).
        extra_fields : list
        List of extra fields (passed by reference).
        default : int, optional
        Default field to return if intent missing.
        multiple : bool, optional
        Allow multiple fields?

        Returns:
        --------
        fieldIDs : str
        Extracted field ID(s), comma-seperated for multiple fields."""

    fields = msmd.fieldsforintent(intent)

    if fields.size == 0:
        logger.warn('Intent "{0}" not found in dataset "{1}". Setting to "{2}"'.format(intent,MS,default))
        fieldIDs = "'{0}'".format(default)
    elif fields.size == 1:
        fieldIDs = "'{0}'".format(fields[0])
    else:
        logger.info('Multiple fields found with intent "{0}" in dataset "{1}" - {2}.'.format(intent,MS,fields))

        if multiple:
            logger.info('Will use all of them for "{0}".'.format(fieldname))
            fieldIDs = "'{0}'".format(','.join([str(fields[i]) for i in range(fields.size)]))
        else:
            maxfield, maxscan = 0, 0
            scans = [msmd.scansforfield(ff) for ff in fields]
            # scans is an array of arrays
            for ind, ss in enumerate(scans):
                if len(ss) > maxscan:
                    maxscan = len(ss)
                    maxfield = fields[ind]

            logger.warn('Only using field "{0}" for "{1}", which has the most scans ({2}).'.format(maxfield,fieldname,maxscan))
            fieldIDs = "'{0}'".format(maxfield)

            #Put any extra fields with intent CALIBRATE_BANDPASS in target field
            extras = list(set(fields) - set(extra_fields) - set([maxfield]))
            if len(extras) > 0:
                logger.warn('Putting extra fields with intent "{0}" in "extrafields" - {1}'.format(intent,extras))
                extra_fields += extras

    return fieldIDs

# ========================================================================================================

def main(args,taskvals):

    visname = va(taskvals, "data", "vis", str)
    fields = read_fields(visname)

    if len(fields)>0:
        config_parser.overwrite_config(args['config'], conf_dict=fields, conf_sec='fields')

        logger.info('[fields] section written to "{0}". Edit this section if you need to change field IDs (comma-separated string for multiple IDs, not supported for calibrators).'.format(args['config']))
        msmd.open(visname)
        fnames = msmd.namesforfields()
        config_parser.overwrite_config(args['config'], conf_dict={'fieldnames': "{0}".format(fnames)}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')

    else:
        logger.info('No updated [fields] section written to "{0}". Edit this section manually (comma-separated string for multiple IDs, not supported for calibrators).'.format(args['config']))

    return

# ========================================================================================================
# ========================================================================================================

if __name__ == '__main__':

    bookkeeping.run_script(main)
