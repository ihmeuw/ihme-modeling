import db_queries


class EnvelopeParameters:

    def __init__(
            self,
            gbd_round_id: int,
            decomp_step: str
    ):
        self.gbd_round_id: int = gbd_round_id
        self.decomp_step: str = decomp_step

        self._run_id: int = db_queries.get_envelope(
            gbd_round_id=self.gbd_round_id, decomp_step=self.decomp_step
        ).run_id.iat[0]

    @property
    def run_id(self) -> int:
        return self._run_id
