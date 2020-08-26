class FilterError(Exception): pass


class FilterPipeline:
    """An object that runs filters on a Pandas DataFrame"""

    def __init__(self, df, logger):
        self._df = df
        self._logger = logger

    def __call__(self, *funcs):
        for func in funcs:
            func_name = func.__name__
            mask = func(self._df)
            
            self._pre_apply_mask(mask, func_name)
            self._df = self._df[~mask]
            self._post_apply_mask(mask, func_name)

        return self._df

    def _pre_apply_mask(self, mask, func_name):
        self._logger.report_filter(func_name, self._df[mask])
        
    def _post_apply_mask(self, mask, func_name):
        pass



