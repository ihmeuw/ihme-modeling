
import argparse
import os
from FILEPATH import split_epi_model


def all_parser(s):
    try:
        s = int(s)
        return s
    except:
        return s

parser = argparse.ArgumentParser(description='Split a parent epi model')
parser.add_argument('source_meid', type=int)
parser.add_argument('--target_meids', type=all_parser, nargs="*")
parser.add_argument('--prop_meids', type=all_parser, nargs="*")
parser.add_argument('--split_measure_ids', type=all_parser, nargs="*",
    default=[5, 6])
parser.add_argument('--prop_meas_id', type=int, default=18)
parser.add_argument(
    '--output_dir',
    type=str,
    default= FILEPATH)
parser.add_argument('--gbd_round_id', type=int, default=6)
parser.add_argument('--decomp_step', type=str, default="step1")
args = vars(parser.parse_args())

try:
    os.makedirs(args['output_dir'])
except:
    pass


#for testing the way arguments are passed
print(args['source_meid'])
print(args['target_meids'])
print(args['prop_meids'])
print(args['split_measure_ids'])
print(args['prop_meas_id'])
print(args['output_dir'])
print(args['gbd_round_id'])
print(args['decomp_step'])


res = split_epi_model(
    source_meid=args['source_meid'],
    target_meids=args['target_meids'],
    prop_meids=args['prop_meids'],
    split_measure_ids=args['split_measure_ids'],
    prop_meas_id=args['prop_meas_id'],
    output_dir=args['output_dir'],
    gbd_round_id=args['gbd_round_id'],
    decomp_step=args['decomp_step'])

print('{}'.format(res))