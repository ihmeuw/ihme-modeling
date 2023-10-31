# File permissions for output files, in python 3 octal syntax
FILE_PERMISSIONS = 0o775
UMASK_PERMISSIONS = 0o002

# STDERR/STDOUT templates
STDERR_PHASE_DIR_TEMPLATE = "{}/stderr/{}"
STDERR_FILE_TEMPLATE = "{}/stderr/{}/{}/stderr-$JOB_ID-$JOB_NAME.txt"
