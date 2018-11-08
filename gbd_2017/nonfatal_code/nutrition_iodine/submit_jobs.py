'''
This part of the code sets up directories to store the draws retrieved from 
the epi database and executes the copy_over.py, 
prep_cretin_dismod_output_for_upload.do, iodine_id_splits.py, 
and save.py scripts. copy_over.py is run first, then 
prep_cretin_dismod_output_for_upload.do, followed by iodine_id_splits.py. After
each data manipulation step, a save step is executed to save the new
data to the database. Comment/uncomment class instantiations as needed
before calling the submit_jobs.py script from the console.
'''

import pandas as pd
import os
import shutil
import json
import re
import subprocess
from db_queries import (get_best_model_versions, 
    get_location_metadata, get_covariate_estimates)
from datetime import datetime

#TODO: Change the username and/or root path so that the person running the 
# code has write privileges to the output directories
username = "USERNAME"
root = "FILEPATH"
error_path = "FILEPATH"
output_path = "FILEPATH"

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)

# make timestamped output folder
date_regex = re.compile('\W')
date_unformatted = str(datetime.now())[0:13]
c_date = date_regex.sub('_', date_unformatted)
base = "FILEPATH"

if not os.path.exists(base):
    os.makedirs(base)

class CopyOver(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold
    
    def execute(self):

        copy_over = {
                "cretinism" : {
                    "srcs": "9428",
                    "trgs": "9428"
                }
            }
        # add subdirectories to timestamped output folder
        out_dir = "FILEPATH"

        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
            os.makedirs(out_dir)
        else:
            os.makedirs(out_dir)

        for identity, me_map in copy_over.items():
            # add me_id output directories to acause subdirectory
            save_ids = []
            for _, v in me_map.items():
                output_meid = v.get('trgs',{})
                if not os.path.exists(os.path.join(out_dir, str(output_meid))):
                    os.makedirs(os.path.join(out_dir, str(output_meid)))
                save_ids.append(output_meid)

        # submit copy_over job for each year and form a string of job namess
        job_string = ''
        for year_id in [1990, 1995, 2000, 2005, 2010, 2017]:
            job_name = 'cret_backfill_{y}'.format(y=year_id)
            job_string = job_string + ',' + job_name
            call = ('qsub -hold_jid {hj} -l mem_free=20.0G -pe multi_slot 10'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' python_shell.sh'
                        ' copy_over.py'
                        ' \'{arg1}\' {arg2} {arg3}'.format(hj=self.hold, 
                            o=output_path, 
                            e=error_path, 
                            jn=job_name, 
                            arg1=json.dumps(copy_over), 
                            arg2=out_dir, arg3=year_id))
            #print call
            subprocess.call(call, shell=True)

        save_string = ''
        # save custom results with hold_jid, hold until years finish above
        for save_id in save_ids:
            description = "prop profound id due to cret copy and backfill for GBD2017"
            job_name = "save_{svid}".format(svid=save_id)
            save_string = save_string + "," + job_name
            call = ('qsub  -hold_jid {hj} -l mem_free=120.0G -pe multi_slot 60'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' python_shell.sh'
                        ' save.py'
                        ' {arg1} {arg2} \'{arg3}\''.format(hj=job_string, 
                            o=output_path, e=error_path, jn=job_name, 
                            arg1=save_id, arg2=out_dir, arg3=description))

            subprocess.call(call, shell=True)
        return save_string

class AdjustIodID(object):
    '''Takes DisMod outputs from ME 2929 
    (Intellectual disability due to iodine deficiency), and does location 
    specific updates and then reuploads those results to 18777 
    (Intellectual disability due to iodine deficiency, adjusted) '''
    def __init__(self, hold='no_holds'):
        self.hold = hold

    def execute(self):
        src_meid = 2929
        src_mvid = 326309
        trg_meid = 18777
        out_dir = os.path.join(base,str(trg_meid))
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
            os.makedirs(out_dir)
        else:
            os.makedirs(out_dir)
        data_dir = base # where the loc_meta data will go
        
        # export data for individual job import to reduce hits to database
        # location_set_id 22 has the same locations as 35
        keeps = ['location_id','super_region_id','super_region_name',
        'region_id','region_name','ihme_loc_id']
        loc_meta = get_location_metadata(location_set_id=35, gbd_round_id=5)
        loc_list = loc_meta.loc[loc_meta.most_detailed==1,'location_id'].tolist()
        loc_meta = loc_meta[keeps]
        loc_meta.to_csv(os.path.join(data_dir, 'location_metadata.csv'), 
            index=False, encoding='utf8')
        iod_salt_cov = 46
        iod_salt = get_covariate_estimates(covariate_id=iod_salt_cov, 
            gbd_round_id=5)
        iod_salt.to_csv(os.path.join(data_dir, 'iod_salt_cov.csv'), index=False, 
            encoding='utf8')

        job_string = ''
        for loc in loc_list:
            job_name = "adjust_iodID_{}".format(loc)
            job_string = job_string + "," + job_name
            call = ('qsub -hold_jid {hj} -pe multi_slot 2'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' stata_shell.sh'
                        ' prep_cretin_dismod_output_for_upload.do'
                        ' {arg1} {arg2} {arg3} {arg4}'.format(hj=self.hold,
                            o=output_path,
                            e=error_path,
                            jn=job_name, 
                            arg1=out_dir,
                            arg2=data_dir,
                            arg3=loc,
                            arg4=src_mvid))
            subprocess.call(call, shell=True)

        # Save the results
        save_hold = job_string
        iod_adj_description = "Adjustments made on meid {}, mvid {}".format(src_meid, src_mvid)
        
        save_params = [
                str(trg_meid), 
                "--description", "\'{}\'".format(iod_adj_description),
                "--input_dir", out_dir, 
                "--best",
                "--sexes", "1", "2",
                "--meas_ids", "5",
                "--file_pattern", "{measure_id}_{location_id}_{year_id}_{sex_id}.csv"]
        
        save_job_name = 'iod_adj_save_{}'.format(trg_meid)
        call = ('qsub -hold_jid {hj} -pe multi_slot 13'
                ' -cwd -P proj_custom_models'
                ' -o {o}'
                ' -e {e}'
                ' -N {jn}'
                ' python_shell.sh'
                ' save.py'
                ' {arg1}'.format(hj=save_hold,
                    o=output_path,
                    e=error_path,
                    jn=save_job_name,
                    arg1=' '.join(save_params)))
        subprocess.call(call, shell=True)
        return save_job_name



class IodineIDSplit(object):
    '''Splits 'Intellectual disability due to iodine deficiency, adjusted' 
    into 'Profound intellectual disability due to iodine deficiency 
    unsqueezed' and 'Severe intellectual disability due to iodine deficiency 
    unsqueezed' '''
    def __init__(self, hold='no_holds'):
        self.hold = hold

    def execute(self):
        
        split_map = {
            "iod": {
                "srcs": {
                    "tot": "18777",
                    "profound_prop": "9428"
                },
                "trgs": {
                    "severe": "9935",
                    "profound": "9936"
                }
            }
        }
        # make server directory
        directory = "{root}/{proc}".format(root=base, proc="split")
        if os.path.exists(directory):
            shutil.rmtree(directory)
            os.makedirs(directory)
        else:
            os.makedirs(directory)

        # add me_id output directories
        save_ids = []
        for _, v in split_map.items():
            outputs = v.get('trgs',{})
            for trg_key, me_id in outputs.items():
                if not os.path.exists(os.path.join(directory, str(me_id))):
                    os.makedirs(os.path.join(directory, str(me_id)))
                save_ids.append(me_id)

        # Get best models versions of parent_ids for save step
        parent_meids = []
        for _, v in split_map.items():
            outputs = v.get('srcs',{})
            for src_key, me_id in outputs.items():
                parent_meids.append(me_id)
        mvid_df = get_best_model_versions(entity='modelable_entity',
            ids=parent_meids, gbd_round_id=5)

        split_params = [
            "--me_map", "\'{}\'".format(json.dumps(split_map)),
            "--out_dir", directory, "--year_id"]

        split_string = ''
        for i in [1990, 1995, 2000, 2005, 2010, 2017]: 
            split_job = "iodine_id_split_{}".format(i)
            split_string = split_string + "," + split_job
            call = ('qsub -hold_jid {hj} -l mem_free=20.0G -pe multi_slot 10'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' python_shell.sh'
                    ' iodine_id_splits.py'
                    ' {arg1}'.format(hj=self.hold,
                        o=output_path,
                        e=error_path,
                        jn=split_job, 
                        arg1=' '.join(split_params + [str(i)])))
            subprocess.call(call, shell=True)

        # save the results
        hold = split_string
        iod_split_description = 'Used following modelable_entity_ids and model_version_ids in split calculation:'
        model_ids_str_for_save = ''
        for index,row in mvid_df.iterrows():
            model_ids_str_for_save += ' meid {}, mvid {};'.format(row.modelable_entity_id,row.model_version_id)
        iod_split_description += model_ids_str_for_save[:-1] + "."
        
        for save_id in save_ids:
            save_params = [
                str(save_id), 
                "--description", "\'{}\'".format(iod_split_description),
                "--input_dir", os.path.join(directory, str(save_id)), 
                "--best",
                "--sexes", "1", "2",
                "--meas_ids", "5",
                "--file_pattern", "{year_id}.h5"]

            call = ('qsub -hold_jid {hj} -l mem_free=120.0G'
                    ' -pe multi_slot 60'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' python_shell.sh'
                    ' save.py'
                    ' {arg1}'.format(hj=hold,
                        o=output_path,
                        e=error_path,
                        jn='iod_save_{}'.format(save_id),
                        arg1=' '.join(save_params)))
            subprocess.call(call, shell=True)

if __name__ == "__main__":
    # Until this can be refactored, draws.py should be changed to use the previous 
    # gbd_round_id when the CopyOver() class executed (e.g. draws.py should use 
    # gbd_round_id 4 if we are in gbd_round_id 5). Change the round_id back
    # to the current round once the copy over and backfill has completed.
    copy = CopyOver()
    copy.execute()
    #adj = AdjustIodID()
    #hold = adj.execute()
    # need to manually change draws.py 
    # to use yjr current gbd_round_id on the split step
    #split = IodineIDSplit()
    #split.execute()