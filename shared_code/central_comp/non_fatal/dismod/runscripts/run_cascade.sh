#!/bin/sh
#$ -S /bin/sh
source /etc/profile.d/sge.sh
user_name=$1
model_id=$2
log_file="strDir.$model_id.$$"
#
echo $* > $log_file
printenv >> $log_file
#
# Need to set current working directory to this particular value
working_dir=strDir

# qsub the pandas version
if [ "$ENVIRONMENT_NAME" = "dev" ]; then
cat << EOF > /tmp/run_dismod.$$
/usr/local/UGE/bin/lx-amd64/qsub \
-N dm_${model_id}_P \
-P proj_dismod \
-v ENVIRONMENT_NAME=$ENVIRONMENT_NAME \
-e strDir \
-o strDir \
${working_dir}/strDir/_run_pandas_dismod_qsub.sh \
${model_id}
EOF
else
cat << EOF > /tmp/run_dismod.$$
strDir/qsub \
-N dm_${model_id}_P \
-P proj_dismod \
-e strDir/errors \
-o strDir/output \
${working_dir}/strDir/_run_pandas_dismod_qsub.sh \
${model_id}
EOF
fi

Pcmd=`cat /tmp/run_dismod.$$`
# Run the panda cascade
echo "sudo -u ${user_name} sh -c source /etc/profile.d/sge.sh;$Pcmd" >> $log_file
sudo -u ${user_name} sh -c ". /etc/profile.d/sge.sh;$Pcmd"
sudo chown $user_name:IHME-users $log_file
sudo chmod 775 $log_file
