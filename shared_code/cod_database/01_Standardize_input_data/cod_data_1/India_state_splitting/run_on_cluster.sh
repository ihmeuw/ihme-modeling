# Use the bash shell to interpret this job script
#$ -S /bin/bash
#

# submit this job to nodes that have
# at least 1GB of RAM free.
#$ -l mem_free=4.0G


## Put the hostname, current directory, and start date
## into variables, then write them to standard output.
GSITSHOST=`/bin/hostname`
GSITSPWD=`/bin/pwd`
GSITSDATE=`/bin/date`
echo "**** JOB STARTED ON $GSITSHOST AT $GSITSDATE"
echo "**** JOB RUNNING IN $GSITSPWD"
##

# make sure that boost library is in the path
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/boost-current/lib


echo calling python -u "$@"
python -u "$@"


## Put the current date into a variable and report it before we exit.
GSITSENDDATE=`/bin/date`
echo "**** JOB DONE, EXITING 0 AT $GSITSENDDATE"
##

## Exit with return code 0
exit 0

