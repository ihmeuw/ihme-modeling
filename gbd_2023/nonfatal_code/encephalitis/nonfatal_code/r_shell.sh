#$ -S /bin/bash
# Run these to avoid autofs mount issues -- this should initialize the entire filesystem under /ihme
ls /ihme/* 1>/dev/null
ls /home/j 1>/dev/null
run_file="$1"; shift
/bin/singularity exec -B /tmp:/tmp /share/singularity-images/rstudio/latest.img /usr/local/bin/R <$run_file  --no-save --args $@