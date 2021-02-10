import os
import warnings

def update_jdls(ms_name,
                ms_loc,
                container_loc,
                user,
                out_base
               ):
    """Updates the keywords in the default jdl to match those of the given input.

    This function prepares the jdl files to save their outputs (OutputData) to:
    out_base/user[0]/user/ms_name
    Note: This file MUST exist before the jdls are submitted or the files should not save (and the folder will not be accessible in the future).

    Arguments:
    ----------
     - ms_name: Name of the balled (.tar.gz) original measurement set file (e.g. "1491550051").
     - ms_loc: Path within the IRIS catalog of the measurement set.
     - container_loc = Folder path of the two containers required for processing (i.e. PATH in PATH/meerkat.xvfb.simg and PATH/casameer-5.4.1.xvfb.simg).
     - user: Path of the user where the output data is saved.
     - out_base: Path at which the user info is input. Usually: "LFN:/skatelescope.eu/user"
    """

    # Prepare useable paths
    output_data_loc = f"{out_base}/{user[0]}/{user}/{ms_name}"
    ms_loc = f"{ms_loc}/{ms_name}.ms.tar.gz"
    if not os.path.isdir("pipelines"):
        os.system("mkdir pipelines")
    #if not os.path.isdir(f"pipelines/{ms_name}"):
    #    os.system(f"mkdir pipelines/{ms_name}")

    while True: # Wait for above to finish
        if os.path.isdir(f"pipelines"):
            break


    # Copy base files and change name of config file
    os.system(f"cp -r processing pipelines/{ms_name} \n")
    while True: # Wait for above to finish
        if os.path.exists(f"pipelines/{ms_name}/default_config.txt"):
            break
        if os.path.exists(f"pipelines/{ms_name}/myconfig.txt"):
            warnings.warn(f"Pipeline already existed, likely will cause unexpected issues.")
            break
    os.system(f"mv pipelines/{ms_name}/default_config.txt pipelines/{ms_name}/myconfig.txt")

    # Iterate through files and append the appropriate paths
    for name in os.listdir(f"pipelines/{ms_name}/"):
        # Use tags to insert correct paths
        if (('meerkat' in name) and ('.jdl' in name)) or ('myconfig.txt' in name):
            path = f"pipelines/{ms_name}/{name}"
            os.system(f"cp {path} .tmp")
            while True: # Wait for above to finish
                if os.path.isfile(".tmp"):
                    break
            with open(path, 'w') as w:
                with open('.tmp', 'r') as r:
                    for idx, line in enumerate(r):
                        line = line.replace("%ms_name", ms_name)
                        line = line.replace("%ms_loc", ms_loc)
                        line = line.replace("%container_loc", container_loc)
                        line = line.replace("%output_data_loc", output_data_loc)
                        w.write(line)

        # Name visibility key in myconfig.txt
        #elif 'myconfig.txt' in name:
        #    path = f"pipelines/{ms_name}/{name}"
        #    os.system(f"cp {path} .tmp")
        #    with open(path, 'w') as w:
        #        with open('.tmp', 'r') as r:
        #            for idx, line in enumerate(r):
        #                if "vis = ''" in line:
        #                    line = line.replace("vis = ''", f"vis = 'data/{ms_name}.ms'")
        #                    w.write(line)

    # Clean up temporary file and print warning
    os.system(f"rm .tmp")
    print(f"""Files created in {ms_name}\nThe .jdl output will be saved to {output_data_loc}
Note: This path must exist before submiting .jdls!""")

if __name__ == "__main__":
    """To create a set of processing files for a given data set,
    adapt the values in PATHS to match those required for your data set.
    update_jdls() docstring details exactly what each key represents.
    NOTE: For the Created .jdl jobs to work, 'out_base/user[0]/user/ms_name'
    must already exist! The exact directory will be printed after the amended
    jdls have been created.
    """

    PATHS = {
        #'ms_name': "1491550051", 'ms_loc': "LFN:/skatelescope.eu/user/p/priyaa.thavasimani/MeerKAT_DataSets", # DEEP2 1491550051
        'ms_name': "1538856059_sdp_l0", 'ms_loc': "LFN:/skatelescope.eu/user/p/priyaa.thavasimani", # XMMLSS12 1538856059
        #'ms_name': "1538942495_sdp_l0", 'ms_loc': "LFN:/skatelescope.eu/user/p/priyaa.thavasimani", # XMMLSS13 1538942495
        'container_loc':"LFN:/skatelescope.eu/user/a/anna.scaife/meerkat",
        'user': "micah.bowles",
        'out_base': "LFN:/skatelescope.eu/user"
    }

    update_jdls(**PATHS)
