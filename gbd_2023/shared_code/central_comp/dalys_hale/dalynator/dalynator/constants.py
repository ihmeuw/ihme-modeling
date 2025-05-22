from typing import Dict

from dual_upload.lib.utils.constants import Contexts
import gbd.constants as gbd


# File permissions for output files, in python 3 octal syntax
FILE_PERMISSIONS = 0o775
UMASK_PERMISSIONS = 0o002

# STDERR/STDOUT templates
STDERR_PHASE_DIR_TEMPLATE = "{}/stderr/{}"
STDERR_FILE_TEMPLATE = "{}/stderr/{}/{}/FILEPATH"

SINGLE_YEAR_TABLE_TYPE = "single_year"
MULTI_YEAR_TABLE_TYPE = "multi_year"
NATOR_TABLE_TYPES = [SINGLE_YEAR_TABLE_TYPE, MULTI_YEAR_TABLE_TYPE]

MEASURE_TYPES = {
    v: k.lower() for k, v in gbd.measures.items() if k in ["DEATH", "DALY", "YLD", "YLL"]
}

PROCESS_NAMES_SHORT: Dict[int, str] = {
    gbd.gbd_process.ETIOLOGY: "eti",
    gbd.gbd_process.RISK: "risk",
    gbd.gbd_process.SUMMARY: "summary",
}

# mapping of gbd process to compare context
COMPARE_CONTEXTS: Dict[int, str] = {
    gbd.gbd_process.ETIOLOGY: Contexts.CompareContexts.ETIOLOGY.value,
    gbd.gbd_process.RISK: Contexts.CompareContexts.RISK.value,
    gbd.gbd_process.SUMMARY: Contexts.CompareContexts.CAUSE.value,
}

PUBLIC_UPLOAD_SUBDIR = "public_upload"
