from db_queries import get_population
import gbd.constants as gbd

from fauxcorrect.utils.constants import LocationSetId


class PopulationParameters:

    def __init__(
            self,
            gbd_round_id: int = gbd.GBD_ROUND_ID,
            decomp_step: str = gbd.decomp_step.ONE,
            location_set_id: int = LocationSetId.OUTPUTS
    ):
        self.gbd_round_id: int = gbd_round_id
        self.decomp_step: str = decomp_step
        self.location_set_id: int = location_set_id

        self._run_id: int = get_population(
            gbd_round_id=self.gbd_round_id, decomp_step=self.decomp_step,
            location_set_id=self.location_set_id
        ).run_id.item()

    @property
    def run_id(self) -> int:
        return self._run_id
