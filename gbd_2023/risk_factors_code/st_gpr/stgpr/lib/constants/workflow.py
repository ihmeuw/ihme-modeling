"""Constants for jobmon workflow."""

from typing import Dict, Final

# ST-GPR tool constants.
TOOL_NAME: Final[str] = "ST-GPR"
ACTIVE_TOOL_VERSION_ID: Final[int] = 96

# Compute resource keys.
MAX_RUNTIME: Final[str] = "runtime"
MEMORY: Final[str] = "memory"
CORES: Final[str] = "cores"
QUEUE: Final[str] = "queue"
CONSTRAINTS: Final[str] = "constraints"
ARCHIVE: Final[str] = "archive"

# Default compute resources.
RESOURCE_SCALES: Dict[str, int] = {MEMORY: 1, MAX_RUNTIME: 1}
HARD_LIMITS: bool = True
ALL_QUEUE: Final[str] = "all.q"

# Workflow-level constants.
WORKFLOW_NAME_TEMPLATE: Final[str] = "ST-GPR version {run_id}"
WORKFLOW_ARGS_TEMPLATE: Final[str] = "stgpr_{run_id}"

# Test-specific workflow_args to avoid name collisions between prod and test runs
WORKFLOW_ARGS_TEMPLATE_TEST: Final[str] = "stgpr_{run_id}_test"

# Workflow run-level constants.
SECONDS_UNTIL_TIMEOUT: Final[int] = 48 * 60 * 60

# Cluster name expected by Jobmon.
SLURM: Final[str] = "slurm"

# Maximum number of parallel jobs in a model stage.
MAX_SUBMISSIONS: Final[int] = 10
