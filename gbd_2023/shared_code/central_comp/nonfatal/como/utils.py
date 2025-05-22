import re
import subprocess
import time
from itertools import islice
from typing import Iterable, Iterator, List

import numpy as np
import pandas as pd

from como.lib import constants


def batched(iterable: Iterable, n: int) -> Iterator:
    """Useful until python 3.12 itertools.batched()"""
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch


def startup_jitter() -> None:
    """Sleeps a random interval within a constant-controlled window."""
    time.sleep(
        np.random.default_rng().uniform(low=0.0, high=constants.MP_STARTUP_JITTER_WINDOW_SEC)
    )


def get_como_branch() -> str:
    """Get the current como branch."""
    lines = subprocess.check_output(["pip", "show", "como"]).decode("utf-8").split("\n")
    line = [i for i in lines if i.startswith("Editable project location:")][0]
    dpath = line.split(":")[-1].strip()
    git_branch = (
        subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=dpath)
        .decode("UTF-8")
        .strip()
    )
    return git_branch


def ordered_draw_columns(df: pd.DataFrame) -> List[str]:
    """Returns list of draw columns in numerical order."""
    draw_cols = list(df.filter(like="draw_").columns)
    numeric = [int(re.findall(r"\d+", c)[0]) for c in draw_cols]
    order = np.argsort(numeric)
    # avoid numpy.str_ type
    draw_cols = [str(i) for i in np.array(draw_cols)[order]]
    return draw_cols
