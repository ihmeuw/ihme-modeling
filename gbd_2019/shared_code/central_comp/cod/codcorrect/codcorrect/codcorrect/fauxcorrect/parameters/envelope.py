from db_queries import get_envelope
import gbd.constants as gbd

# This is OK to keep for both codecorrect and fauxcorrect. CoDCorrect
# just uses get_envelope under the hood to get the best envelope version id
# any way
class EnvelopeParameters:

    def __init__(
            self,
            gbd_round_id: int = gbd.GBD_ROUND_ID,
            decomp_step: str = gbd.decomp_step.ONE
    ):
        self.gbd_round_id: int = gbd_round_id
        self.decomp_step: str = decomp_step

        self._run_id: int = get_envelope(
            gbd_round_id=self.gbd_round_id, decomp_step=self.decomp_step
        ).run_id.item()

    @property
    def run_id(self) -> int:
        return self._run_id