import os
import warnings

def update_file(filename, tag, inp_list, outfile = None):
    """Updates lines of a file which contain tag to contain multiple lines
    which cover the whole inp_list.
    Arguments:
	----------
    filename : str
		The name of the file to be update.
    tag : str
		The key which will be used to determine which lines (and
    	where) need to be updated.
    inp_list : list of str, or str
	 	The elements which will replace the tag, if list new lines will be
		appended with same each element.
    outfile : str
		Name of the file to write to, standard is to overwrite filename (None).
    """
    if outfile==None:
        outfile = filename
    with open(filename, 'r') as file:
        content = file.readlines()
    # Check each line for tag and replace / append as reuquired
    for idx, line in enumerate(content):
        found = line.find(tag)
        if found > -1:
            if type(inp_list)==list:
                tmp = line.replace(tag, inp_list[0])
                if len(inp_list)>1:
                    tmp = tmp.rstrip(" \n,")+",\n"
                    for n in inp_list[1:]:
                        tmp += line.replace(tag, n).rstrip(" \n,")+",\n"
            else:
                tmp = line.replace(tag, inp_list)
            content[idx] = tmp
    out = "".join(content) # String for writing out.
    out = out.replace(",\n}", "\n}") # Remove commas if at end of a block.
    with open(outfile, 'w') as w:
        w.write(out)

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
     ms_name : str
        Name of the balled (.tar.gz) original measurement set file (e.g. "1491550051").
     ms_loc : str
        Path within the IRIS catalog of the measurement set.
     container_loc : str
        Folder path of the two containers required for processing (i.e. PATH in PATH/meerkat.xvfb.simg and PATH/casameer-5.4.1.xvfb.simg).
     user :
        Path of the user where the output data is saved.
     out_base : str
        Path at which the user info is input. Usually: "LFN:/skatelescope.eu/user"
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
    tags = [
        ["%ms_name", ms_name],
        ["%ms_loc", ms_loc],
        ["%container_loc", container_loc],
        ["%output_data_loc", output_data_loc]
    ]
    for name in os.listdir(f"pipelines/{ms_name}/"):
        # Use tags to insert correct paths
        if (('meerkat' in name) and ('.jdl' in name)) or ('myconfig.txt' in name):
            path = f"pipelines/{ms_name}/{name}"
            for tag in tags:
                update_file(path, tag[0], tag[1])

    # Find nloops
    config_path = f"pipelines/{ms_name}/myconfig.txt"
    path = f"pipelines/{ms_name}/meerkat_clean.sh"

    # Updating Selfcal scripts according to nloops:
    with open(config_path, 'r') as r:
        content = r.readlines()


    # Clean up temporary file and print warning
    print(f"""Files created in {ms_name}\nThe .jdl output will be saved to {output_data_loc}
Note: This path must exist before submiting .jdls!""")

def update_clean_sh(ms_name):
    """Updates the clean.sh script to contain the correct number selfcalibration loops."""
    file_path = f"pipelines/{ms_name}/meerkat_clean.sh"
    with open(f"pipelines/{ms_name}/myconfig.txt", 'r') as r:
        content = r.readlines()
    for line in content:
        if line[:6] == "nloops":
            nloops = int(line.split()[2])
    # Generate correct order & number of selfcal, bdsf and pixmask calls.
    scripts_ = ["selfcal_part1.py", "selfcal_part2.py", "run_bdsf.py", "make_pixmask.py"]
    scripts = []
    scripts = scripts_*nloops + "selfcal_final.py"

    # Use list of calls to generate correct bash calls.
    base_mpi = f"time singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C casameer-5.4.1.xvfb.simg mpicasa -n $OMP_NUM_THREADS casa -c selfcal_scripts/SCRIPT_NAME --config myconfig.txt\n"
    base_single_thread = f"time singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C casameer-5.4.1.xvfb.simg xvfb-run -d casa --log2term -c selfcal_scripts/SCRIPT_NAME --config myconfig.txt\n"
    python_single_thread = f"time singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C casameer-5.4.1.xvfb.simg python selfcal_scripts/SCRIPT_NAME --config myconfig.txt\n"
    # Generate full list of commands to append for selfcalibration.
    secondary_loop = False
    content = ""
    for idx, script_name in enumerate(scripts):
        if idx%len(scripts_)==0:
            content +='\n'
        content += f'echo ">>> executing {script_name} on data"\n'
        # Use the correct call for each script
        if "selfcal_part1.py" == script_name or "selfcal_final.py" == script_name:
            content += base_mpi.replace("SCRIPT_NAME", script_name)
        elif "run_bdsf.py" == script_name:
            content += python_single_thread.replace("SCRIPT_NAME", script_name)
            # If a full loop has occured, then we can remove the previous working documents
            if secondary_loop:
                content += 'echo ">>> cleaning directory"\nrm -r *.mms_im_*\n'
            else:
                secondary_loop = True
        else:
            content += base_single_thread.replace("SCRIPT_NAME", script_name)
    update_file(file_path, "%script_calls", content)

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
    
    if PATHS['ms_name'] == "1491550051":
        do_pol = 'False'
    else:
        do_pol = 'True'
    
    update_jdls(**PATHS)
    update_clean_sh(PATHS["ms_name"])
    update_file(f"pipelines/{PATHS["ms_name"]}/myconfig.txt", '%do_pol', do_pol)

