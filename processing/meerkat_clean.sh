# ========================================================
#!/bin/bash
echo "==START=="
/bin/hostname
echo "======="
/bin/ls -la
echo "======="
/bin/date
echo "======="

echo "kernel version check:"
uname -r
echo "shell: $0"

echo "Job ID: $1"

echo "SPW: $3"

echo "No. threads: "
echo $OMP_NUM_THREADS

echo "printing singularity version on grid:"
singularity --version
unset LD_LIBRARY_PATH

cat /proc/meminfo
free -m

# ========================================================
echo "Extracting Process Monitor - This is to monitor the processes that we will run"
mkdir prmon && tar -xvf prmon_1.0.1_x86_64-static-gnu72-opt.tar.gz -C prmon --strip-components 1
echo "Running prmon"
./prmon/bin/prmon -p $$ -i 10 &

# ========================================================
echo ">>> extracting scripts"
tar -xvzf IRISprocessMeerKAT.tar.gz
/bin/mv IRISprocessMeerKAT/* .
echo ">>> scripts successfully extracted"

# ========================================================
# extract concatenated data set (i.e. unzip from concat file)
#tar -czvf combinedMMS.tar.gz *.mms
#tar -cvzf combinedFITS.tar.gz *.fits
#tar -cvzf combinedCUBE.tar.gz *.contcube
echo ">>> extracting MMS"
tar -xzvf combinedMMS.tar.gz
/bin/ls -lah

echo ">>> extracting caltables"
for file in caltables_*.tar.gz; do
tar -xvzf $file
dirname="${file%.tar.gz}"
end=${#dirname}
spw="${dirname:10:$end}"
/bin/mkdir $spw/caltables
/bin/mv caltables/* $spw/caltables/
done

# ========================================================

/bin/rm *.tar.gz
/bin/ls -lah
echo ">>> data sets successfully extracted"
# ========================================================

#echo ">>> executing concat on data"
#time singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C casameer-5.4.1.xvfb.simg casa --log2term -c aux_scripts/concat.py --config myconfig.txt

echo ">>> listing folders to show what concat.py has created"
/bin/ls -lah

# Should make config file contain field ids - Need the ids *before* I submit the jdl. -.-
#echo ">>> executing get_fields on data"
#time singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C meerkat.xvfb.simg casa --log2term -c aux_scripts/get_fields.py --config myconfig.txt
%script_calls

#echo ">>> executing plotcal_spw on data"
#time singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C casameer.1-5.4.xvfb.simg xvfb-run -d casa --log2term -c crosscal_scripts/plotcal_spw.py --config myconfig.txt

# ========================================================
# create outputs:

cp myconfig.txt myconfig_$1.txt
tar -cvzf combinedCLEANED_full_slfcal.tar.gz *_im_*

/bin/ls -lah
