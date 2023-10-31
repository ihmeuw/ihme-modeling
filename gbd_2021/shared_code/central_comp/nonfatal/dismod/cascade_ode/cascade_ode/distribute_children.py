import os
from itertools import repeat
from concurrent.futures import ProcessPoolExecutor as Pool


def distribute(worker, to_share, work):
    """
    Runs children in parallel processes, sharing a dictionary of
    data with the children. The work is a list of single items.
    The worker's signature is ``f(shared_dictionary, location_id)``

    Args:
        worker (function): Function to call in subprocess.
        to_share (dict): Dictionary of data to share.
        work (List[int]): List of location IDs.

    Returns:
        List[object]: List of results from the worker functions.
    """
    fthread = int(os.environ.get("SGE_HGR_fthread", os.cpu_count()))
    worker_cnt = min(fthread, len(work), 10)
    with Pool(worker_cnt) as pool:
        result = pool.map(worker, list(zip(repeat(to_share), work)))
    return result
