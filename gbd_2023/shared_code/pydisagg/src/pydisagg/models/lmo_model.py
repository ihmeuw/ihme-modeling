from pydisagg.models.disagg_model import DisaggModel
from pydisagg.transformations import LogModifiedOdds


class LMOModel(DisaggModel):
    """DisaggModel using the log-modified odds transformation with the exponent m."""

    def __init__(self, m: float) -> None:
        super().__init__(transformation=LogModifiedOdds(m))
