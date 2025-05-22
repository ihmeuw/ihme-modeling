"""Mapping from Marketscan employee location of residence (egeoloc) to GBD location_id.

This is a 1:1 map between Marketscan's proprietary "egeoloc" state/territory code and
GBD location_ids. This version of the map has been used since at least GBD2015 and it should
not need to be modified unless there is some type of change to the political boundaries
of the U.S.
"""

import pandas as pd

EGEOLOC_LOCATION_DICT = {
    59: 573,
    20: 572,
    39: 571,
    65: 570,
    38: 569,
    9: 568,
    58: 567,
    49: 566,
    44: 565,
    28: 564,
    37: 563,
    8: 562,
    13: 561,
    64: 560,
    48: 559,
    19: 558,
    27: 557,
    36: 556,
    12: 555,
    57: 554,
    11: 553,
    7: 552,
    56: 551,
    26: 550,
    55: 549,
    25: 548,
    43: 547,
    24: 546,
    18: 545,
    6: 544,
    35: 543,
    5: 542,
    47: 541,
    42: 540,
    23: 539,
    22: 538,
    17: 537,
    16: 536,
    54: 535,
    63: 534,
    34: 533,
    33: 532,
    31: 531,
    32: 530,
    4: 529,
    53: 528,
    62: 527,
    46: 526,
    52: 525,
    61: 524,
    41: 523,
    98: 422,
    97: 385,
    1: 102,
}

EGEOLOC_LOCATION_DF = (
    pd.DataFrame.from_dict(EGEOLOC_LOCATION_DICT, orient="index")
    .reset_index(drop=False)
    .rename(columns={"index": "egeoloc", 0: "location_id"})
)
