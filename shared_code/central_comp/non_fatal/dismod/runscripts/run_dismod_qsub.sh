#!/bin/sh
#$ -S /bin/sh
source /etc/profile.d/sge.sh
user_name=$1
model_id=$2
push_results_into_database=1
log_file="strDir/${user_name}/qsub_log.$$"
#
echo $* > $log_file
#
# Need to set current working directory to this particular value
working_dir=strDir

# qsub the pandas version
cat << EOF > /tmp/run_dismod.$$
strDir/qsub \
-N dm_${model_id}_P \
-P proj_dismod \
-e strDir/errors \
-o strDir/output \
${working_dir}/strDir/_run_pandas_dismod_qsub.sh \
${model_id}
EOF

Pcmd=`cat /tmp/run_dismod.$$`
# Run the panda cascade
echo "sudo -u ${user_name} sh -c source /etc/profile.d/sge.sh;$Pcmd" >> $log_file
sudo -u ${user_name} sh -c ". /etc/profile.d/sge.sh;$Pcmd"
