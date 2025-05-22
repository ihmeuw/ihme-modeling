# this part uploads the code to the epi database
from save_results import save_results_epi
import argparse
import os

def all_parser(s):
    try:
        s = int(s)
        return s
    except:
        return s

# Parse arguments
parser = argparse.ArgumentParser(description="""
    Aggregate draws up a location hierarchy and upload to the epi db""")
parser.add_argument(
    'meid', type=int, help='modelable_entity_id to be uploaded')
parser.add_argument(
    '--description', type=str, help='upload description', default='')
parser.add_argument(
    '--input_dir', type=str, help='input directory where draws are saved',
    default='')
parser.add_argument(
    '--years', type=int, nargs='*', help='years to upload',
    default=[1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022, 2023, 2024])
parser.add_argument(
    '--best', help='mark the uploaded model as "best"',
    action='store_true')
parser.add_argument(
    '--sexes', type=int, nargs='*', help='sexes to upload', default=[1, 2])
parser.add_argument(
    '--meas_ids', type=all_parser, nargs="*", default=[5, 6])
parser.add_argument(
    '--metric_id', type=int, help='are values in count space or rate space?',
    default=3)
parser.add_argument(
    '--file_pattern',
    type=str,
    help=(
        'string specifying the general pattern used in draw filenames, '
        'where special idenifying fields (e.g. location_id, or sex_id) '
        'are enclosed in curly braces {}. For example, a valid file '
        'pattern might be: {location_id}_{year_id}_{sex_id}.csv. Note '
        'that if you are using h5 files, you will also need to specify '
        'an h5_tablename'),
    default="{location_id}.h5")
parser.add_argument(
    '--env',
    type=str,
    help='dev/prod environment',
    default='prod')
parser.add_argument(
    '--gbd_round_id',
    type=int,
    help='gbd round id',
    default=7)
parser.add_argument(
    '--decomp_step',
    type=str,
    help='the decomposition step of the saved results',
    default='step1')
parser.add_argument(
    '--crosswalk_version_id',
    type=int,
    help="The crosswalk version id for the corresponding me_id")
parser.add_argument(
    '--bundle_id',
    type=int,
    help="The bundle id for the corresponding me_id")

args = vars(parser.parse_args())

#for testing the way arguments are passed
'''
print(args['meid'])
print(args['input_dir'])
print(args['description'])
print(args['file_pattern'])
print(args['years'])
print(args['sexes'])
print(args['best'])
print(args['meas_ids'])
print(args['metric_id'])
print(args['env'])
print(args['gbd_round_id'])
print(args['decomp_step'])
print(args['crosswalk_version_id'])
print(args['bundle_id'])
'''

model_version_df = save_results_epi(modelable_entity_id=args['meid'],
                                    input_dir=args['input_dir'],
                                    description=args['description'],
                                    input_file_pattern=args['file_pattern'],
                                    year_id=args['years'],
                                    sex_id=args['sexes'],
                                    mark_best=args['best'],
                                    measure_id=args['meas_ids'],
                                    metric_id=args['metric_id'],
                                    n_draws=1000,
                                    db_env=args['env'],
                                    gbd_round_id=args['gbd_round_id'],
                                    decomp_step=args['decomp_step'],
                                    crosswalk_version_id=args['crosswalk_version_id'],
                                    bundle_id=args['bundle_id'])
