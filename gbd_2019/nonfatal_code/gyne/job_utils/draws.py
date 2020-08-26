import pandas as pd
import collections
from job_utils import getset
from get_draws.api import get_draws
from db_queries import get_location_metadata, get_demographics
from inspect import signature
from functools import wraps
import gbd.constants as gbd


##############################################################################
# Decorator for enforcing types of arguments in method calls
# from Python Cookbook 3rd Edition
##############################################################################
def typeassert(*ty_args,**ty_kwargs):
    def decorate(func):
        # If in optimized mode, disable type checking
        if not __debug__:
            return func

        # Map function argument names to supplied types
        sig = signature(func)
        bound_types = sig.bind_partial(*ty_args, **ty_kwargs).arguments

        @wraps(func)
        def wrapper(*args, **kwargs):
            bound_values = sig.bind(*args, **kwargs)
            # Enforce type assertions across supplied arguments
            for name, value in bound_values.arguments.items():
                if name in bound_types:
                    if not isinstance(value, bound_types[name]):
                        raise TypeError(
                            'Argument {} must be {}'.format(name, bound_types[name]))
            return func(*args, **kwargs)
        return wrapper
    return decorate

##############################################################################
# Make sure data comes in square
##############################################################################
class ClassProperty(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()


class SquareImport(object):
    _epi_demographics = get_demographics("epi", gbd_round_id=gbd.GBD_ROUND_ID)
    _idx_dmnsns = {
        "year_id": _epi_demographics['year_id'],
        "age_group_id": _epi_demographics['age_group_id'],
        "sex_id": [1, 2],
        "location_id": _epi_demographics['location_id'],
        "measure_id": [5, 6]
    }

    _draw_cols = ["draw_{i}".format(i=i) for i in range(0, 1000)]

    def __init__(self, decomp_step, idx_dmnsns=None, draw_cols=None):
        self.decomp_step = decomp_step
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

    def import_square(self, gbd_id_type, gbd_id, source, filler=None, **kwargs):
        """get draws for the specified modelable entity by dimensions"""
        if not kwargs:
            kwargs = self.idx_dmnsns.copy()

        if filler is None:
            filler = 0

        df = get_draws(gbd_id_type=gbd_id_type,
            gbd_id=gbd_id,
            source=source,
            measure_id=kwargs['measure_id'],
            location_id=kwargs['location_id'],
            year_id=kwargs['year_id'],
            age_group_id=kwargs['age_group_id'],
            sex_id=kwargs['sex_id'],
            gbd_round_id=gbd.GBD_ROUND_ID,
            decomp_step = self.decomp_step)

        for c in self.idx_dmnsns.keys():
            df[c] = pd.to_numeric(df[c])
        df = df.set_index(list(self.idx_dmnsns.keys()))
        df = df[self.draw_cols]
        df = pd.concat([self.index_df, df], axis=1)
        df.fillna(value=filler, inplace=True)
        return df
