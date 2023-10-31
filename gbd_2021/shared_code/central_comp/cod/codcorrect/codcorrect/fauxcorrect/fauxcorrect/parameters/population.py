import db_queries


class PopulationParameters:

    def __init__(
            self,
            gbd_round_id: int,
            decomp_step: str,
            location_set_id: int
    ):
        self.gbd_round_id: int = gbd_round_id
        self.decomp_step: str = decomp_step
        self.location_set_id: int = location_set_id

        self._run_id: int = db_queries.get_population(
            gbd_round_id=self.gbd_round_id, decomp_step=self.decomp_step,
            location_set_id=self.location_set_id
        ).run_id.iat[0]

    @property
    def run_id(self) -> int:
        return self._run_id
