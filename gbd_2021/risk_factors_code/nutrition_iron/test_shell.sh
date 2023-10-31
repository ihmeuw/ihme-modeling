#$ -S /bin/sh

export PATH="FILEPATH:$PATH"

FILEPATH/python "$@"
source FILEPATH/activate gbd_env
