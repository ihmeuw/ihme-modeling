import os
import shutil
import subprocess
import sys

hardy_types = ('btt', 'ett', 'sct', 'hemi')
type_dict = {'btt':([2085, 0], 2484), 'ett': ([2087, 0], 2488), 'sct': ([2097, 2103], 2501), 'hemi':([2112, 0], 2505)}
year_list = [1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019]

root_dir = 'FILEPATH'

for hardy_type in hardy_types:
    out_meid = type_dict[hardy_type][1]
    out_dir = root_dir + str(out_meid)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    else:
        shutil.rmtree(out_dir)
        os.makedirs(out_dir)
    first_arg = type_dict[hardy_type][0][0]
    second_arg = type_dict[hardy_type][0][1]
    hardy_type = hardy_type
    for year_id in year_list:
        job_name = "hardy_{t}_{y}".format(t=hardy_type, y=str(year_id))
	call = ('qsub -pe multi_slot 5'
                ' -cwd -P proj_anemia -o'
                ' FILEPATH' + 
                ' {0} {1} {2} {3} {4}'.format(hardy_type, out_dir, first_arg, second_arg, year_id))
        subprocess.call(call, shell=True)
