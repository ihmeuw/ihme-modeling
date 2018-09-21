"""
Name of Script: split_worker.py
Description: Worker function to submit split_epi_model jobs
Arguments: --source - model id of model you want to split
           --targs  - ids for newly split models
           --props  - ids for models that contain the proportions on which
                      to split the source
           --outdir - where to save the resulting .h5 files of the split models
"""


import sys
from transmogrifier.draw_ops import split_epi_model
import argparse


def split_worker(source_meid, target_meids, prop_meids, output_dir):
    split_epi_model(source_meid=source_meid,
                    target_meids=target_meids,
                    prop_meids=prop_meids,
                    output_dir=output_dir)


def main():
    args = parse_args()
    assert(len(args.props) == len(args.targs)), ("Number of proportion model IDS don't"
                                                 "equal number of target model ids.")
    split_worker(args.source,
                 args.targs,
                 args.props,
                 args.outdir)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',
                        type=int,
                        action='store',
                        help='the source_meid')

    parser.add_argument('--targs',
                        nargs='*',
                        type=int,
                        help='target_meids')
    parser.add_argument('--props',
                        nargs='*',
                        type=int,
                        help='prop_meids')
    parser.add_argument('--outdir',
                        action='store',
                        help='the output directory')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
