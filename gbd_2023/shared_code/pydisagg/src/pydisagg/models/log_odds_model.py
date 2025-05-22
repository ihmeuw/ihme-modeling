from pydisagg.models.disagg_model import DisaggModel
from pydisagg.transformations import LogOdds


class LogOddsModel(DisaggModel):
    """Produces an DisaggModel assuming multiplicativity in the odds"""

    def __init__(self) -> None:
        super().__init__(transformation=LogOdds())
