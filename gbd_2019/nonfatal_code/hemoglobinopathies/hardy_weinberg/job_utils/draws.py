import pandas as pd
import collections
from job_utils import getset
from get_draws.api import get_draws

##############################################################################
# Make sure data comes in square
##############################################################################


class ClassProperty(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()


class SquareImport(object):

    _idx_dmnsns = {
        "year_id": [1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019],
        #"age_group_id": getset.get_age_group_set(12)["age_group_id"],
        "age_group_id": [i for i in range(2, 21, 1)] + [30, 31, 32, 235, 164],
        "sex_id": [1, 2],
        "location_id": getset.get_most_detailed_location_ids(),
        "measure_id": [5, 6]
    }

    _draw_cols = ["draw_{i}".format(i=i) for i in range(0, 1000)]

    def __init__(self, idx_dmnsns=None, draw_cols=None):

        if idx_dmnsns is None:
            self.idx_dmnsns = collections.OrderedDict(
                sorted(self.default_idx_dmnsns.items()))
        else:
            self.idx_dmnsns = collections.OrderedDict(
                sorted(idx_dmnsns.items()))

        if draw_cols is None:
            self.draw_cols = self.default_draw_cols
        else:
            self.draw_cols = draw_cols

        # expected index
        self.index_df = self.get_index_df()

    @ClassProperty
    @classmethod
    def default_idx_dmnsns(cls):
        return cls._idx_dmnsns.copy()

    @ClassProperty
    @classmethod
    def default_draw_cols(cls):
        return cls._draw_cols[:]

    def get_index_df(self):
        """create template index for square dataset"""
        idx = pd.MultiIndex.from_product(
            self.idx_dmnsns.values(),
            names=self.idx_dmnsns.keys())
        return pd.DataFrame(index=idx)

    def import_square(self, meid, source, filler=None, **kwargs):
        """get draws for the specified modelable entity by dimensions"""
        if not kwargs:
            kwargs = self.idx_dmnsns.copy()

            # spillover from needing to replace keys with their plural form for gopher.draws
            for k in kwargs.keys():
                kwargs[k] = kwargs.pop(k)

        if filler is None:
            filler = 0

        df = get_draws('modelable_entity_id', meid, source=source, status='latest', decomp_step='step1', **kwargs)
        for c in self.idx_dmnsns.keys():
            df[c] = pd.to_numeric(df[c])
        df = df.set_index(self.idx_dmnsns.keys())
        df = df[self.draw_cols]
        df = pd.concat([self.index_df, df], axis=1)
        df.fillna(value=filler, inplace=True)
        return df
