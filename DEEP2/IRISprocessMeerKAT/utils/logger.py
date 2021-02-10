import config_parser
import logging
from time import gmtime

logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s")
logger.setLevel(logging.DEBUG)

def setup_logger(config,verbose=False):
    
    """Setup logger at debug or info level according to whether verbose option selected (via command line or config file).
        
        Arguments:
        ----------
        config : str
        Path to config file.
        verbose : bool
        Verbose output? This will display all logger debug output."""
    
    #Overwrite with verbose mode if set to True in config file
    if not verbose:
        config_dict = config_parser.parse_config(config)[0]
        if 'slurm' in config_dict.keys() and 'verbose' in config_dict['slurm']:
            verbose = config_dict['slurm']['verbose']

    loglevel = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(loglevel)

    return logger
