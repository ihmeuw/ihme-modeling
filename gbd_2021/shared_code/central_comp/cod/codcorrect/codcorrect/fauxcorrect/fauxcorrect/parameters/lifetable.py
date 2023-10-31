from fauxcorrect.queries import pred_ex
from fauxcorrect.utils.constants import MortalityProcessId


class LifeTableParameters:
    """
    Records the relevant external parameters that go into
    predicted life expectancy (pred ex), which are:
        * Life tables, with shock (id 29)
        * Theoretical minimum risk life table (TMRLT, id 30)

    FAQ:
        What are expected changes to life expectancy that we should anticipate?
            TMRLT may not really change until the end of the round. In fact,
            it might not even be updated each round. But LE will change a lot.

            Overall not likely not to vary a lot from run-to-run.

        How do we pull predicted life expectancy?
            pred_ex is stored in the mortality db in the
            'upload_theoretical_minimum_risk_life_table_estimate' table.
            However, what's not stored is the life table with 'age_at_death'
            and 'pred_ex' merged together, which is what we must do.

            Prior to GBD 2020, we used a sproc (mortality.get_pred_ex)
            that did the merge in SQL on the fly, but took > 1 hr.

            We now use a translation of the sproc into Python that the mortality
            team passed us, but be aware that we should check periodically
            that we are still pulling the right thing.
    """

    def __init__(
            self,
            gbd_round_id: int,
            decomp_step: str
    ):
        self.gbd_round_id: int = gbd_round_id
        self.decomp_step: str = decomp_step

        # best run id for life tables
        self.life_table_run_id: int = pred_ex.get_best_mortality_process_run_id(
            MortalityProcessId.LIFE_TABLE_WITH_SHOCK, self.gbd_round_id,
            self.decomp_step
        )

        # best run id for theoretical minimum risk life table (TMRLT)
        self.tmrlt_run_id: int = pred_ex.get_best_mortality_process_run_id(
            MortalityProcessId.TMRLT, self.gbd_round_id, self.decomp_step
        )
