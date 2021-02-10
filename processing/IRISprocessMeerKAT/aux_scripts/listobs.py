import os, sys
sys.path.append(os.getcwd())

import logging
from time import gmtime
logging.Formatter.converter = gmtime

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.DEBUG)

from utils import config_parser
from utils.config_parser import validate_args as va
from utils import bookkeeping

# ========================================================
# ========================================================

def main(args,taskvals):
    
    visname = va(taskvals, 'data', 'vis', str)
    name = visname.split('/')[-1].replace('.ms','')
    
    listobs(vis=visname, listfile=name+"_listobs.txt")

    return

# ========================================================
# ========================================================

if __name__ == "__main__":
    bookkeeping.run_script(main)
