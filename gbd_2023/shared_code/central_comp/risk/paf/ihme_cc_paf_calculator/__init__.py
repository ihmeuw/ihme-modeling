"""PAF Calculator."""

from importlib.metadata import version

try:
    __version__ = version(__name__)
except Exception:  # pragma: no cover
    __version__ = "unknown"


from ihme_cc_paf_calculator.api.public import get_paf_model_status  # noqa F401
from ihme_cc_paf_calculator.cli.launch_paf_calculator import (  # noqa F401
    launch_paf_calculator,
)
from ihme_cc_paf_calculator.lib.constants import PafModelStatus  # noqa: F401
from ihme_cc_paf_calculator.lib.custom_pafs.hiv import (  # noqa: F401
    calculate_hiv_due_to_drug_use_pafs,
)
from ihme_cc_paf_calculator.lib.custom_pafs.injuries import (  # noqa: F401
    calculate_injury_pafs,
)
