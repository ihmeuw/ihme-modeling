from typing import List

import pandas as pd


def splice_data(current_data: pd.DataFrame, previous_data: pd.DataFrame, splice_locs: List[int]):
    previous_data = previous_data.reset_index().set_index(current_data.index.names)
    previous_data = previous_data.loc[:, current_data.columns]
    keep_locs = list(set(current_data.reset_index().location_id).difference(splice_locs))
    spliced_data = pd.concat([current_data.loc[keep_locs], previous_data.loc[splice_locs]])
    return spliced_data.sort_index()
