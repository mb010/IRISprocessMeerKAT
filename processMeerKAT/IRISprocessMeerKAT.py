#!/usr/bin/env python2.7
from utils import config_tools as ct
from utils import kwarg_tools as kt
from utils import slurm as slurm
from utils import iris as iris
from utils import globals
from utils.logger import setup_logger
from utils.kwarg_tools import parse_args


def main():
    
    #Parse command-line arguments, and setup logger
    args = parse_args()
    setup_logger(args.config,args.verbose)
    
    if args.iris:
        write_jobs = iris.write_jobs_iris
        default_config = ct.default_config_iris
        format_args = kt.format_args_iris
    else:
        write_jobs = slurm.write_jobs
        default_config = ct.default_config
        format_args = kt.format_args

    #Mutually exclusive arguments - display version, build config file or run pipeline
    if args.version:
        logger.info('This is version {0}'.format(globals.__version__))
    if args.license:
        logger.info(license)
    if args.build:
        default_config(vars(args))
    if args.run:
        kwargs = format_args(args.config,args.submit,args.quiet,args.dependencies)
        write_jobs(args.config, **kwargs)

    return

if __name__ == "__main__":
    main()
