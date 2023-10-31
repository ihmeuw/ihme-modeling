import os
import sys
from contextlib import contextmanager
import hashlib

import pandas as pd
import numpy as np


@contextmanager
def suppress_stdout():
    with open(os.devnull, 'w') as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:  
            yield
        finally:
            sys.stdout = old_stdout
            
            
def parent_inheritance(data: pd.DataFrame, hierarchy: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(data, pd.DataFrame):
        raise ValueError(f'Needs to provide DataFrame, not {type(data)}.')
    if data.index.names != ['location_id']:
        raise ValueError('Index can only be `location_id`.')
    location_ids = hierarchy['location_id'].to_list()
    path_to_top_parents = hierarchy['path_to_top_parent'].to_list()
    path_to_top_parents = [list(reversed(p.split(',')[:-1])) for p in path_to_top_parents]
    for location_id, path_to_top_parent in zip(location_ids, path_to_top_parents):
        if location_id not in data.reset_index()['location_id'].to_list():
            for parent_id in path_to_top_parent:
                try:
                    data = data.append(data.loc[int(parent_id)].rename(location_id))
                    break
                except KeyError:
                    pass
    
    return data


def text_wrap(text: str, splitter: str = ' ', line_length: int = 30) -> str:
    new_text = ''
    line = 0
    for w in text.split(splitter):
        new_text += f'{w}'
        new_text += splitter
        line += len(w)
        if line > line_length:
            new_text += '\n'
            line = 0
            
    if line == 0:
        new_text = new_text[:-(1 + len(splitter))]
    else:
        new_text = new_text[:-len(splitter)]
    
    return new_text


def get_random_seed(key: str):
    seed = int(hashlib.sha1(key.encode('utf8')).hexdigest(), 16) % 4294967295
    
    return seed

def get_random_state(key: str):
    seed = get_random_seed(key)
    random_state = np.random.RandomState(seed=seed)
    
    return random_state
