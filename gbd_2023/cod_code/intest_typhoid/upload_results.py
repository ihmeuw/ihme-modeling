import argparse
from save_results import save_results_epi, save_results_cod

parser = argparse.ArgumentParser()
parser.add_argument('--type', type = str, required = True)
parser.add_argument('--id', type = int, required = True)
parser.add_argument('--path', type = str, required = True)
parser.add_argument('--pattern', type = str, default = '{location_id}.csv', required = False)
parser.add_argument('--description', nargs = '+', type = str, required = True)
parser.add_argument('--metric', type = int, default = 3, required = False)
parser.add_argument('--measure', nargs = '+', type = int, required = True)
parser.add_argument('--best', default = 'False', required = False)
parser.add_argument('--release', type = int, required = True)
parser.add_argument('--bundle', type = int, required = False)
parser.add_argument('--xwalk', type = int, required = False)

args = parser.parse_args()
best = args.best=='True'
description = ' '.join(args.description)

if (args.type == 'epi'):
    if (args.bundle is None):
        parser.error('If type is "epi" then bundle is required.')
    elif (args.xwalk is None):
        parser.error('If type is "epi" then xwalk is required.')
    else:
       print("is epi") 
       mv = save_results_epi(input_dir = args.path, input_file_pattern= args.pattern, modelable_entity_id = args.id, 
                              description = description, measure_id = args.measure, metric_id = args.metric, 
                              release_id = args.release, mark_best = best, bundle_id = args.bundle, crosswalk_version_id = args.xwalk)

elif (args.type == 'cod'):
    print('is cod')
    mv = save_results_cod(input_dir = args.path, input_file_pattern = args.pattern, cause_id = args.id, description = description, 
                          metric_id = args.metric, release_id = args.release, mark_best = best)
    
print(best)
print(description)
print(vars(args))
print(mv)
