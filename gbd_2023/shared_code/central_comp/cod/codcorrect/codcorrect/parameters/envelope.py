"""Envelope parameters class.

CoDCorrect uses get_envelope under the hood to get the best envelope version id.
"""

import db_queries

from codcorrect.legacy.utils import constants


class EnvelopeParameters:

    def __init__(
            self,
            release_id: int
    ):
        self.release_id: int = release_id

        self._run_id: int = db_queries.get_envelope(
            release_id=self.release_id,
            with_hiv=constants.MortalityEnvelope.WITH_HIV,
            use_rotation=False
        ).run_id.iat[0]

    @property
    def run_id(self) -> int:
        return self._run_id
