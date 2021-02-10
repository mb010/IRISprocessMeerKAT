# ---------------------------------------------------------------------
# History
# [200717 - Anna Scaife] : created
# [212301 - Micah Bowles] : Updated for cleaning / Memory limitations
# ---------------------------------------------------------------------

import os,sys
import time


# options for debugging purposes
precal = False
runcal = False
slfcal = False
concat = True
clean = True

# ---------------------------------------------------------------------
# ---------------------------------------------------------------------

def get_jobid(filename):

	tmpfile = open(filename,"r")
	items = tmpfile.readline().split()
	if len(items) > 3:
		for item in items: item.rstrip(',').rstrip(']').lstrip('[')
		jobid = items[2:]
	else:
		jobid = items[-1]
	tmpfile.close()

	return jobid

# ---------------------------------------------------------------------

def get_status(filename):

	finished = False; success = False
	tmpfile = open(filename,"r")
	output = tmpfile.readline().split(';')[0].split('=')[-1]
	if output=="Done":
		finished = True
		success = True
	elif output=="Failed":
		finished = True
		success = False
	tmpfile.close()

	return finished, success

# ---------------------------------------------------------------------

def run_precal():

	# submit job:
	os.system("dirac-wms-job-submit meerkat_precal.jdl > .tmp \n")
	jobid_precal = get_jobid('.tmp')
	print("Running precal: " + str(jobid_precal))

	# get job status:
	while True:
		os.system("dirac-wms-job-status "+jobid_precal+" > .tmp\n")
		finished, success = get_status('.tmp')

		if finished:
			print("get_status returned finished and success=",success)
			break

	# check status:
	if success:
		print("precal:", success)

		# update config file:
		os.system("dirac-wms-job-get-output "+jobid_precal+" \n")
		os.system("mv "+jobid_precal+"/myconfig* .tmp")
		for i, line in enumerate(open('.tmp')):
			if line.find("bpassfield")>-1: bpassfield = line.split()[-1]
			if line.find("fluxfield")>-1: fluxfield = line.split()[-1]
			if line.find("phasecalfield")>-1: phasecalfield = line.split()[-1]
			if line.find("targetfields")>-1: targetfields = line.split()[-1]
			if line.find("extrafields")>-1: extrafields = line.split()[-1]
			if line.find("polfield")>-1: polfield = line.split()[-1]


		os.system("mv myconfig.txt .tmp \n")
		configfile = open('myconfig.txt',"w")
		for i, line in enumerate(open('.tmp')):
			if line.find("bpassfield")>-1:
				tmp = line.split()[-1]
				line = line.replace(tmp,bpassfield)
			if line.find("fluxfield")>-1:
				tmp = line.split()[-1]
				line = line.replace(tmp,fluxfield)
			if line.find("phasecalfield")>-1:
				tmp = line.split()[-1]
				line = line.replace(tmp,phasecalfield)
			if line.find("targetfields")>-1:
				tmp = line.split()[-1]
				line =line.replace(tmp,targetfields)
			if line.find("extrafields")>-1:
				tmp = line.split()[-1]
				line = line.replace(tmp,extrafields)
			if line.find("polfield")>-1:
				tmp = line.split()[-1]
				line = line.replace(tmp,polfield)
			configfile.write(line)

		configfile.close()
		os.system("rm -r "+jobid_precal+" \n")
		os.system("rm -r .tmp \n")

	else:
		print("precal:", success)
		print("Check logs to determine error. JobID: "+jobid_precal)
		sys.exit()


	return success

# ---------------------------------------------------------------------

def run_runcal():

	# submit job:
	os.system("dirac-wms-job-submit meerkat_runcal.jdl > .tmp \n")
	jobid_runcal = get_jobid('.tmp')
	print("Running runcal: "+" ".join(jobid_runcal))

	# get job status:
	while True:
		finished, success = [], []
		for jobid in jobid_runcal:
			jobid = jobid.rstrip(',').rstrip(']').lstrip('[')
			os.system("dirac-wms-job-status "+jobid+" > .tmp\n")
			f, s = get_status('.tmp')
			finished.append(f)
			success.append(s)

		if all(finished):
			break

	# check status:
	if all(success):
		print("runcal:", success[0])

		# update config file:
		os.system("dirac-wms-job-get-output "+jobid+" \n")
		os.system("mv "+jobid+"/myconfig* .tmp")
		for i, line in enumerate(open('.tmp')):
			if line.find("fieldnames")>-1: os.system('echo "'+line+'" >> myconfig.txt \n')
		os.system("rm -r "+jobid+" \n")

	else:
		print("runcal: False")
		print("Check logs to determine error.")
		sys.exit()

	return

# ---------------------------------------------------------------------

def run_slfcal():

	# submit job:
	os.system("dirac-wms-job-submit meerkat_slfcal.jdl > .tmp \n")
	jobid_slfcal = get_jobid('.tmp')
	print("Running slfcal: "+jobid_slfcal)

	# get job status:
	while True:
		os.system("dirac-wms-job-status "+jobid_slfcal+" > .tmp\n")
		finished, success = get_status('.tmp')
		if finished:
			break

	# check status:
	if success:
		print("slfcal:", success)
	else:
		print("slfcal:", success)
		print("Check logs to determine error. JobID: "+jobid_slfcal)
		sys.exit()

		os.system("rm .tmp \n")

	return

# ---------------------------------------------------------------------

def run_concat():

	# submit job:
	os.system("dirac-wms-job-submit meerkat_concat.jdl > .tmp \n")
	jobid_slfcal = get_jobid('.tmp')
	print("Running concat: "+jobid_slfcal)

	# get job status:
	while True:
		os.system("dirac-wms-job-status "+jobid_slfcal+" > .tmp\n")
		finished, success = get_status('.tmp')
		if finished:
			break

	# check status:
	if success:
		print("concat:", success)
	else:
		print("concat:", success)
		print("Check logs to determine error. JobID: "+jobid_slfcal)
		sys.exit()

		os.system("rm .tmp \n")

	return

# ---------------------------------------------------------------------

def run_clean():


	# update config file for clean:
	#os.system("dirac-wms-job-get-output "+jobid_precal+" \n") # Gets output files
	os.system("cp myconfig.txt .config_tmp \n")
	for i, line in enumerate(open('.config_tmp')):
		if line.find("targetfields")> -1: targetfield_int = int(line.split()[-1].strip(",[]'"))
		if line.find("fieldnames")> -1: targetfield_str = line.split()[2:][targetfield_int].strip("',[]")

	configfile = open('myconfig.txt',"w")
	for i, line in enumerate(open('.config_tmp')):
		if line.find("vis")>-1:
			vis_old= line.split()[-1]
			observation_number = vis_old.split("/")[-1].split(".")[0]
			vis_new = "'"+observation_number+"."+targetfield_str+".mms'"
			line = line.replace(vis_old, vis_new)
		configfile.write(line)

	configfile.close()
	# submit job:
	os.system("dirac-wms-job-submit meerkat_clean.jdl > .tmp \n")
	os.system("cp .config_tmp myconfig.txt")
	os.system("rm -r .config_tmp \n")

	jobid_clean = get_jobid('.tmp')
	print(jobid_clean)
	print("Running clean: "+jobid_clean)

	# get job status:
	while True:
		os.system("dirac-wms-job-status "+jobid_clean+" > .tmp\n")
		finished, success = get_status('.tmp')
		if finished:
			break

	# check status:
	if success:
		print("clean:", success)
	else:
		print("clean:", success)
		print("Check logs to determine error. JobID: "+jobid_clean)
		sys.exit()

		os.system("rm .tmp \n")

	return



# ---------------------------------------------------------------------
# ---------------------------------------------------------------------


if __name__ == '__main__':

	start = time.time()

	if precal:
		time_precal = time.time()
		run_precal()
		print("precal time:\t",time.time() - time_precal, " seconds")
	if runcal:
		time_precal = time.time()
		run_runcal()
		print("runcal time:\t",time.time() - time_precal, " seconds")
	if slfcal:
		time_precal = time.time()
		run_slfcal()
		print("slfcal time:\t",time.time() - time_precal, " seconds")
	if concat:
		time_precal = time.time()
		run_concat()
		print("concat time:\t",time.time() - time_precal, " seconds")
	if clean:
		time_clean = time.time()
		run_clean()
		print("clean time:\t",time.time() - time_clean, " seconds")
	end = time.time()

	print("Pipeline complete")
	print("Run time"+str(end-start)+" seconds")
