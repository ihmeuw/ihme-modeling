"""
Name of Script: save_worker.py
Description: Worker function to submit save_custom_results jobs.
Arguments: --meid (int)  - the model id number
           --desc (str)  - the description for the model
           --indir (str) - the directory that contains the .h5 files created by
                           the shared function split_epi_model
           Arguments are required although flagged as optional.
Output: split epi models for causes specified are uploaded to epi viz and
        marked 'best'
"""


import sys
from adding_machine.agg_locations import save_custom_results
import argparse


def save_worker(meid, description, input_dir):
    save_custom_results(meid=meid,
                        description=description,
                        input_dir=input_dir,
                        env='prod',
                        mark_best=True,
                        custom_file_pattern='{location_id}.h5',
                        h5_tablename='draws')


def main():
    args = parse_args()
    print "Working on %d (%s) in %s" % (args.meid[0], args.desc, args.indir[0])
    save_worker(args.meid[0], args.desc, args.indir[0])


class StrJoin(argparse.Action):
    """Argparse action class to join a list into a space-separate string
    """
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, ' '.join(values))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--meid',
                        nargs=1,
                        action='store',
                        type=int,
                        help='the model id number')
    parser.add_argument('--desc',
                        nargs='*',
                        action=StrJoin,
                        help='the description')
    parser.add_argument('--indir',
                        nargs=1,
                        action='store',
                        help='input directory')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
