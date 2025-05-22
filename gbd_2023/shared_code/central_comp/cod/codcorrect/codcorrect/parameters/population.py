import db_queries


class PopulationParameters:

    def __init__(
            self,
            release_id: int,
            location_set_id: int
    ):
        self.release_id: int = release_id
        self.location_set_id: int = location_set_id

        self._run_id: int = db_queries.get_population(
            release_id=self.release_id,
            location_set_id=self.location_set_id,
            use_rotation=False
        ).run_id.iat[0]

    @property
    def run_id(self) -> int:
        return self._run_id
