#! /bin/bash
#$ -S /bin/bash

model_id=$1
export PATH=strCodeDir:$PATH
source activate cascade_ode
echo "python strCodeDir/run_all.py $model_id"
python strCodeDir/run_all.py $model_id
python strCodeDir/run_all.py $model_id
