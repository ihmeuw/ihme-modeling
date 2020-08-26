import os
import shutil

# Path to the repo top-level directory.
CODE_ROOT: str = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../../../"))

# Get path to conda-installed poetry.
POETRY_EXECUTABLE: str = shutil.which("poetry")

# Where probability of death runs are stored.
POD_ROOT: str = "FILEPATH"

# Where temporary files are stored for the current probability of death run.
POD_TMP: str = os.path.join(POD_ROOT, "tmp")

# Where individual output files live.
OUTPUT_FORMAT: str = os.path.join(POD_TMP, "{location_id}.csv")

# Where data is infiled from.
INFILE_FORMAT: str = os.path.join(POD_TMP, "infile_{chunk_number}.csv")
