"""Constants for jobmon workflow."""
from typing import Dict

# ST-GPR tool constants
TOOL_NAME: str = "ST-GPR"
ACTIVE_TOOL_VERSION_ID: int = 10

# Executor parameters
MAX_RUNTIME: str = "max_runtime_seconds"
MEM_FREE: str = "m_mem_free"
NUM_CORES: str = "num_cores"
QUEUE: str = "queue"
J_RESOURCE: str = "j_resource"

# Default executor parameters
RESOURCE_SCALES: Dict[str, int] = {
    MEM_FREE: 1,
    MAX_RUNTIME: 1
}
HARD_LIMITS: bool = True
ALL_QUEUE: str = "all.q"

# Workflow-level constants
WORKFLOW_NAME_TEMPLATE: str = "ST-GPR version {run_id}"
WORKFLOW_ARGS_TEMPLATE: str = "stgpr_{run_id}"

# Workflow run-level constants
SECONDS_UNTIL_TIMEOUT: int = 48 * 60 * 60
