import os,sys
import globals

def check_path(path,update=False):
    
    """Check in specific location for a script or container, including in bash path, and in this pipeline's calibration
        scripts directory (SCRIPT_DIR/{CALIB_SCRIPTS_DIR,AUX_SCRIPTS_DIR}/). If path isn't found, raise IOError, otherwise return the path.
        
        Arguments:
        ----------
        path : str
        Check for script or container at this path.
        update : bool, optional
        Update the path according to where the file is found.
        
        Returns:
        --------
        path : str
        Path to script or container (if path found and update=True)."""
    
    #Attempt to find path firstly in CWD, then directory up, then pipeline directories, then bash path.
    if os.path.exists(path) and path[0] != '/':
        path = '{0}/{1}'.format(os.getcwd(),path)
    if not os.path.exists(path) and path != '':
        if os.path.exists('../{0}'.format(path)):
            newpath = '../{0}'.format(path)
        elif os.path.exists('{0}/{1}'.format(globals.SCRIPT_DIR,path)):
            newpath = '{0}/{1}'.format(globals.SCRIPT_DIR,path)
        elif os.path.exists('{0}/{1}/{2}'.format(globals.SCRIPT_DIR,globals.CALIB_SCRIPTS_DIR,path)):
            newpath = '{0}/{1}/{2}'.format(globals.SCRIPT_DIR,globals.CALIB_SCRIPTS_DIR,path)
        elif os.path.exists('{0}/{1}/{2}'.format(globals.SCRIPT_DIR,globals.AUX_SCRIPTS_DIR,path)):
            newpath = '{0}/{1}/{2}'.format(globals.SCRIPT_DIR,globals.AUX_SCRIPTS_DIR,path)
        elif os.path.exists('{0}/{1}/{2}'.format(globals.SCRIPT_DIR,globals.SELFCAL_SCRIPTS_DIR,path)):
            newpath = '{0}/{1}/{2}'.format(globals.SCRIPT_DIR,globals.SELFCAL_SCRIPTS_DIR,path)
        elif os.path.exists(check_bash_path(path)):
            newpath = check_bash_path(path)
        else:
            #If it still doesn't exist, throw error
            raise IOError('File "{0}" not found.'.format(path))
    else:
        newpath = path
    
    if update:
        return newpath
    else:
        return path


def check_bash_path(fname):
    
    """Check if file is in your bash path and executable (i.e. executable from command line), and prepend path to it if so.
        
        Arguments:
        ----------
        fname : str
        Filename to check.
        
        Returns:
        --------
        fname : str
        Potentially updated filename with absolute path prepended."""
    
    PATH = os.environ['PATH'].split(':')
    for path in PATH:
        if os.path.exists('{0}/{1}'.format(path,fname)):
            if not os.access('{0}/{1}'.format(path,fname), os.X_OK):
                raise IOError('"{0}" found in "{1}" but file is not executable.'.format(fname,path))
            else:
                fname = '{0}/{1}'.format(path,fname)
            break

    return fname
