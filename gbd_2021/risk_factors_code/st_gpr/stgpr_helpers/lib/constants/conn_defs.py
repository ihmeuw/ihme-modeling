"""ST-GPR connection definitions."""
try:
    from typing import Final
except ImportError:
    from typing_extensions import Final

EPI: Final[str] = "epi-uploader"
JOBMON_STGPR: Final[str] = "jobmon-2"
STGPR: Final[str] = "stgpr-uploader"
STGPR_TEST: Final[str] = "stgpr-uploader-test"
