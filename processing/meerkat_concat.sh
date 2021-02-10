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
# full dataset in data directory
mkdir data
/bin/mv *.ms.tar.gz data
cd data
echo ">>> extracting data"
COMMAND="ls *.ms.tar.gz"
for FILE in `eval $COMMAND`
do
tar -xzvf $FILE
done
/bin/ls -la
/bin/rm *.ms.tar.gz
cd ..
echo ">>> data set successfully extracted"
# ========================================================
# mms data in SPW directory / images in images directory
echo ">>> extracting images"

for file in images_*.tar.gz; do
tar -xvzf $file
dirname="${file%.tar.gz}"
end=${#dirname}
spw="${dirname:7:$end}"
/bin/mkdir $spw
/bin/mkdir $spw/images
/bin/mkdir $spw/caltables
/bin/mv images/* $spw/images/
/bin/rm -r images
done

echo ">>> extracting caltables"

for file in caltables_*.tar.gz; do
tar -xvzf $file
dirname="${file%.tar.gz}"
end=${#dirname}
spw="${dirname:10:$end}"
/bin/mv caltables/* $spw/caltables/
/bin/rm -r images
done

echo ">>> extracting MMS data"

for file in outputMMS_*.tar.gz; do
tar -xvzf $file
dirname="${file%.tar.gz}"
end=${#dirname}
spw="${dirname:10:$end}"
/bin/mv *mms* $spw
done

/bin/rm *.tar.gz
/bin/ls -lah
echo ">>> data sets successfully extracted"
# ========================================================

echo ">>> executing concat on data"
time singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C casameer-5.4.1.xvfb.simg casa --log2term -c aux_scripts/concat.py --config myconfig.txt

echo ">>> executing plotcal_spw on data"
time singularity exec --cleanenv --contain --home $PWD:/srv --pwd /srv -C casameer-5.4.1.xvfb.simg xvfb-run -d casa --log2term -c crosscal_scripts/plotcal_spw.py --config myconfig.txt

# ========================================================
# create outputs:

cp myconfig.txt myconfig_$1.txt
tar -czvf fullSPWplots.tar.gz plots
tar -czvf combinedMMS.tar.gz *.mms
tar -cvzf combinedFITS.tar.gz *.fits
tar -cvzf combinedCUBE.tar.gz *.contcube

/bin/ls -la
