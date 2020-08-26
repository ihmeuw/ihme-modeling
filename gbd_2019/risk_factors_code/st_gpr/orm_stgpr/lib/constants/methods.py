from typing import Dict, Tuple, Callable

from gbd.constants import decomp_step as ds
from orm_stgpr.lib.util import offset

# Note: these maps and their wrappers in helpers are separate
# from step_control in order to use them in the same file as
# the implemented methods functions

# METHODS
OFFSET: str = 'offset'

# Maps GBD round ID -> set of decomp steps for which old methods are used
USE_OLD_METHODS_MAP: Dict[int, Tuple[int, ...]] = {
    5: (ds.ITERATIVE),
    6: (ds.ONE, ds.TWO),
    7: (ds.TWO)
}

# Maps method name ->
# GBD round ID ->
# TRUE/FALSE for whether to use updated methods/not ->
# correct method function
# The idea is to be able to pull the right function version
# for a GBD round and decomp step.
# This table does not need to be updated for each round if
# a particular method does not change as the most recently
# updated version is used.
LOOKUP_METHODS_MAP: Dict[str, Dict[int, Dict[bool, Callable]]] = {
    OFFSET:
        {
            6:
            {
                True: offset.offset_data_old,
                False: offset.offset_data
            },
        }
}
