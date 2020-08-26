#!/bin/sh
#$ -S /bin/sh
source /ihme/code/central_comp/miniconda/bin/activate codcorrect

python $1 $2 $3 $4 $5 $6 $7 $8 $9
