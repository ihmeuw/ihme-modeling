import pandas as pd
import collections
from get_draws.api import get_draws
from db_queries import get_location_metadata, get_demographics

##############################################################################
# Make sure data comes in square
##############################################################################


class ClassProperty(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()


class SquareImport(object):
    _epi_demographics = get_demographics("epi", gbd_round_id=5)
    _idx_dmnsns = {
        "year_id": _epi_demographics['year_id'],
        "age_group_id": _epi_demographics['age_group_id'],
        "sex_id": [1, 2],
        "location_id": _epi_demographics['location_id'],
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

    def import_square(self, gopher_what, source, filler=None, **kwargs):
        """get draws for the specified modelable entity by dimensions"""
        if not kwargs:
            kwargs = self.idx_dmnsns.copy()

        if filler is None:
            filler = 0

        df = get_draws(gbd_id_type=gopher_what.keys(),
            gbd_id=gopher_what.values(),
            source=source,
            measure_id=kwargs['measure_id'],
            location_id=kwargs['location_id'],
            year_id=kwargs['year_id'],
            age_group_id=kwargs['age_group_id'],
            sex_id=kwargs['sex_id'],
            gbd_round_id=5)

        for c in self.idx_dmnsns.keys():
            df[c] = pd.to_numeric(df[c])
        df = df.set_index(self.idx_dmnsns.keys())
        df = df[self.draw_cols]
        df = pd.concat([self.index_df, df], axis=1)
        df.fillna(value=filler, inplace=True)
        return df
