"""
Pulling predicted life expectancy.
"""
from codcorrect.lib import db
from codcorrect.legacy.utils.constants import MortalityProcessId


class LifeTableParameters:
    """
    Records the relevant external parameters that go into
    predicted life expectancy (pred ex).
    """

    def __init__(
            self,
            release_id: int
    ):
        self.release_id: int = release_id

        # best run id for life tables
        self.life_table_run_id: int = db.get_best_mortality_process_run_id(
            MortalityProcessId.LIFE_TABLE_WITH_SHOCK, self.release_id
        )

        # best run id for theoretical minimum risk life table (TMRLT)
        self.tmrlt_run_id: int = db.get_best_mortality_process_run_id(
            MortalityProcessId.TMRLT, self.release_id
        )
