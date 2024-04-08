#$ -S /bin/sh
export SINGULARITYENV_OMP_NUM_THREADS=1
export SINGULARITYENV_orig_umask=$(umask)
run_file="$1"; shift
singularity exec -B /tmp:/tmp FILEPATH /IMAGE.img FILEPATH  <$run_file  --no-save --args $@
