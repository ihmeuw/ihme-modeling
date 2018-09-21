import pandas as pd
import collections
from job_utils import getset
from transmogrifier import gopher

##############################################################################
# Make sure data comes in square
##############################################################################


class ClassProperty(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()


class SquareImport(object):

    _idx_dmnsns = {
        "year_id": [1990, 1995, 2000, 2005, 2010, 2016],
        "age_group_id": getset.get_age_group_set(12)["age_group_id"],
        "sex_id": [1, 2],
        "location_id": getset.get_most_detailed_location_ids(),
        "measure_id": [5, 6]
    }

    _draw_cols = ["draw_{i}".format(i=i) for i in range(0, 1000)]

    def __init__(self, idx_dmnsns=None, draw_cols=None):

        if idx_dmnsns is None:
            self.idx_dmnsns = collections.OrderedDict(
                sorted(self.dUSERt_idx_dmnsns.items()))
        else:
            self.idx_dmnsns = collections.OrderedDict(
                sorted(idx_dmnsns.items()))

        if draw_cols is None:
            self.draw_cols = self.dUSERt_draw_cols
        else:
            self.draw_cols = draw_cols

        # expected index
        self.index_df = self.get_index_df()

    @ClassProperty
    @classmethod
    def dUSERt_idx_dmnsns(cls):
        return cls._idx_dmnsns.copy()

    @ClassProperty
    @classmethod
    def dUSERt_draw_cols(cls):
        return cls._draw_cols[:]

    def get_index_df(self):
        """create template index for square dataset"""
        idx = pd.MultiIndex.from_product(
            self.idx_dmnsns.values(),
            names=self.idx_dmnsns.keys())
        return pd.DataFrame(index=idx)

    def import_square(self, gopher_what, source, filler=None, **kwargs):
        """get draws for the specified modelable entity by dimensions"""
        if not kwargs:
            kwargs = self.idx_dmnsns.copy()

            # replace keys with their plural form for gopher.draws
            for k in kwargs.keys():
                kwargs[k + "s"] = kwargs.pop(k)

        if filler is None:
            filler = 0

        df = gopher.draws(gopher_what, source=source, verbose=False,
                          **kwargs)
        for c in self.idx_dmnsns.keys():
            df[c] = pd.to_numeric(df[c])
        df = df.set_index(self.idx_dmnsns.keys())
        df = df[self.draw_cols]
        df = pd.concat([self.index_df, df], axis=1)
        df.fillna(value=filler, inplace=True)
        return df
