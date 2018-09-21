import argparse
import sys
import os
root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
sys.path.append(root)

from lib.job_utils import parsers
from lib.ex_adjust import ExAdjust


def exclusivity_adjuster(me_map, out_dir, year_id):
    """callable function for running exclusivity adjustments"""
    # set dimensionality
    dim = ExAdjust.dUSERt_idx_dmnsns
    dim["year_id"] = year_id

    # run adjustment
    adjuster = ExAdjust(idx_dmnsns=dim, me_map=me_map, out_dir=out_dir)
    adjuster.adjust()
    adjuster.export()


if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--me_map", help="json style string map of ops",
                        required=True, type=parsers.json_parser)
    parser.add_argument("--out_dir", help="root directory to save stuff",
                        required=True)
    parser.add_argument("--year_id", help="which year to use",
                        type=parsers.int_parser, nargs="*", required=True)
    args = vars(parser.parse_args())

    # call exclusivity adjuster
    exclusivity_adjuster(
        me_map=args["me_map"], out_dir=args["out_dir"],
        year_id=args["year_id"])
