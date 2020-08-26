from db_queries import get_life_table
import gbd.constants as gbd

class LifeTableParameters:

    def __init__(
            self,
            gbd_round_id: int = gbd.GBD_ROUND_ID,
            decomp_step: str = gbd.decomp_step.ONE
    ):
        self.gbd_round_id: int = gbd_round_id
        self.decomp_step: str = decomp_step
        self.location_id: int = 1

        lt_run = get_life_table(
            location_id=self.location_id,
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            with_shock=0,
            with_hiv=0
        ).run_id.unique()

        if len(lt_run) < 1:
            raise RuntimeError("Life table returned empty run_id")
        elif len(lt_run) > 1:
            raise RuntimeError("Life table returned more than one run_id")

        self._run_id: int = lt_run[0]

    @property
    def run_id(self) -> int:
        return self._run_id