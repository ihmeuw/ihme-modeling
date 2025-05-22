from typing import Any, Tuple, Union

import numpy as np
import pandas as pd

from inpatient.Upload.tools.logger import ClinicalLogger


class FilterPipeline:
    """An object that runs filters on a Pandas DataFrame"""

    def __init__(self, df: pd.DataFrame, logger: ClinicalLogger):
        self._df = df
        self._logger = logger

    def __call__(self, *funcs: Tuple[Any, ...]):
        for func in funcs:
            func_name = func.__name__
            mask = func(self._df)

            self._pre_apply_mask(mask, func_name)
            self._df = self._df[~mask]
            self._post_apply_mask(mask, func_name)

        return self._df

    def _pre_apply_mask(self, mask: Union[pd.Series, np.ndarray], func_name: str):
        self._logger.report_filter(func_name, self._df[mask])

    def _post_apply_mask(self, mask: Union[pd.Series, np.ndarray], func_name: str):
        pass
