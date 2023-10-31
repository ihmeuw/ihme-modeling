import pandas as pd
from pandas.io.json import json_normalize
import os
import shutil
import json
import re
import subprocess
from job_utils.nch_db_queries import get_bundle_xwalk_version_for_best_model
from db_queries import get_best_model_versions
from db_queries import get_location_metadata
from datetime import datetime
import time
import getpass

# set round id
gbdrid = 7
decomp_step = 'iterative'

# set up stdout and stderr
username = getpass.getuser()
root = os.path.join('FILEPATH', username)
error_path = os.path.join(root, 'errors')
output_path = os.path.join(root, 'output')

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)


date_unformatted = time.strftime('%m_%d_%Y')

base = os.path.join(root, 'congenital_core/{}'.format(date_unformatted))

# import JSON file with me_id source and target information 
code_dir = os.path.dirname(os.path.abspath(__file__))
filepath = os.path.join(code_dir,'congenital_map.json')
with open(filepath, 'r') as f:
        cmap = json.load(f)

# get location_ids to parallelize over
loc_meta = get_location_metadata(location_set_id=35, gbd_round_id=gbdrid, 
    decomp_step=decomp_step)
loc_meta = loc_meta.loc[loc_meta.most_detailed==1, ['location_id', 
    'ihme_loc_id']]
most_detailed_locs = loc_meta.location_id.tolist()

# start code execution loop
for cause_name in ['cong_heart', 'cong_digestive', 'cong_msk', 'cong_chromo', 'cong_neural']:

    me_map = cmap[cause_name]

    ###########################################################################
    # subdirectories
    ###########################################################################
    
    # add acause subdirectories to timestamped output folder
    out_dir = "{base}/{proc}".format(base=base, proc=cause_name)
    
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # add me_id output directories to acause subdirectory
    save_ids = dict()
    for grp, grp_dict in me_map.items(): 
        for ns in grp_dict.keys(): # name short
            try:
                if ns == "Anencephaly":
                    me_id = grp_dict[ns]['trgs']["custom"]
                else: 
                    me_id = grp_dict[ns]['trgs']['post_sqzd_me_id']
                if not os.path.exists(os.path.join(out_dir, str(me_id))):
                    os.makedirs(os.path.join(out_dir, str(me_id)))
                try:
                    save_ids[me_id] = get_bundle_xwalk_version_for_best_model(
                        grp_dict[ns]['srcs']["pre_sqzd_me_id"], gbdrid, decomp_step)
                except LookupError as exc:
                    if grp == 'other':
                        save_ids[me_id] = get_bundle_xwalk_version_for_best_model(
                            next(iter(me_map['envelope'].values()))['srcs']["pre_sqzd_me_id"], gbdrid, decomp_step)
                    else:
                        print((f"Lookup error when getting bundle for ",
                            f"ME {grp_dict[ns]['srcs']['pre_sqzd_me_id']} for {ns}"))
                        raise exc.with_traceback(exc.__traceback__)
                save_ids.append(me_id)
            except KeyError:
                pass

    ###########################################################################
    # Calculate proportion other from claims data
    ###########################################################################
    if cause_name in ['cong_heart', 'cong_chromo']:
        other_job_name = 'calc_other_{}'.format(cause_name)
        call = ('qsub -hold_jid {hj} -l m_mem_free=1G -l fthread=1 -l archive -q long.q'
                        ' -cwd -P proj_nch'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' python_shell.sh'
                        ' calc_other.py'
                        ' \'{arg1}\' {arg2}'.format(hj='no_holds', 
                            o=output_path, e=error_path, 
                            jn=other_job_name, arg1=json.dumps(me_map),
                            arg2=cause_name))
        subprocess.call(call, shell=True)

    ###########################################################################
    # core - custom anceph, squeeze, and heart failure impairment
    ###########################################################################
    # submit congenital_core job for each cause and form a string of job names
    job_string = ''
    for location_id in most_detailed_locs:
        if cause_name == 'cong_neural':
            # calcute the denominator needed for custom anencephaly prevalence
            # calculation
            ihme_loc_id = loc_meta.loc[loc_meta.location_id==location_id,
                'ihme_loc_id'].item()
            denom_job_name = 'denom_calc_{l}_{cn}'.format(l=location_id, 
                cn=cause_name)
            # another person on the MNCH team needs the outputs from this 
            # scripts so place them in a mutually accessible location
            # on the cluster
            share_dir = 'FILEPATH'
            call = ('qsub -hold_jid no_holds -l m_mem_free=2G -l fthread=1 -l archive -q long.q'
                    f' -cwd -P proj_nch'
                    f' -o {output_path}'
                    f' -e {error_path}'
                    f' -N {denom_job_name}'
                    f' python_shell.sh'
                    f' calc_denominator.py'
                    f' {share_dir} {location_id} {gbdrid} {decomp_step}')
            subprocess.call(call, shell=True)
            hold = denom_job_name
            description =  ("Used following modelable_entity_ids and"
                " model_version_ids in target calculation:")
        
        elif cause_name in ['cong_heart', 'cong_chromo']:
            hold = other_job_name
            description = ("Used claims data proportions to calculate 'other'."
                " Copied the following modelable_entity_ids and"
                "  model_version_ids to target meids:")
        
        else:
            hold = 'no_holds'
            description = ("Used subtraction method to calculate 'other'."
                " Used following modelable_entity_ids and model_version_ids"
                " in target calculation:")

        # hold = 'no_holds'
        job_name = 'squeeze_{cn}_{l}'.format(l=location_id, cn=cause_name)
        job_string = job_string + ',' + job_name
        call = (f'qsub -hold_jid {hold}'
                    f' -l m_mem_free=3G -l fthread=2 -l archive -q long.q'
                    f' -cwd -P proj_nch'
                    f' -o {output_path}'
                    f' -e {error_path}'
                    f' -N {job_name}'
                    f' python_shell.sh'
                    f' congenital_core.py'
                    f' \'{json.dumps(me_map)}\' {out_dir}'
                    f' {location_id} {cause_name}'
                    f' {gbdrid} {decomp_step}')
        subprocess.call(call, shell=True)

    ###########################################################################
    # save
    ###########################################################################
    '''Get version ids of models used in squeeze calculations. Upload this 
    information in the save description. This strategy assumes that no new 
    dismod source models have been marked best since the start of the core 
    code.'''

    model_version_list = []
    anceph = []
    for grp, grp_dict in me_map.items():
        for ns in grp_dict.keys(): # name short
            if grp != 'other' and ns != 'Anencephaly':
                me_id = grp_dict[ns]['srcs']['pre_sqzd_me_id']
                model_version_list.append(me_id)
            elif ns == 'Anencephaly':
                anceph.append(grp_dict[ns]['srcs']['pre_sqzd_me_id'])

    df = get_best_model_versions(entity='modelable_entity',
        ids=model_version_list, gbd_round_id=gbdrid, decomp_step = decomp_step)
    model_ids_str_for_save = ''
    for index,row in df.iterrows():
        model_ids_str_for_save += ' meid {}, mvid {};'.format(
            row.modelable_entity_id, row.model_version_id)
    description += model_ids_str_for_save[:-1] + "."
    if anceph:
        df = get_best_model_versions(entity='modelable_entity',
            ids=anceph, gbd_round_id=gbdrid, decomp_step = decomp_step)
        description += " Anenceph used meid {}, mvid {}.".format(
            df.modelable_entity_id.item(),df.model_version_id.item())

    # save custom results with hold_jid
    save_string = ''
    for save_id, save_info in save_ids.items():
        print(save_id)
        job_string= f"'squeeze_{cause_name}_*'"
        job_name = f"save_{cause_name}_{save_id}"
        save_string = save_string + ',' + job_name
        call = (f'qsub  -hold_jid {job_string} -l m_mem_free=60G'
                    f' -l fthread=30 -l archive -q long.q'
                    f' -cwd -P proj_nch'
                    f' -o {output_path}'
                    f' -e {error_path}'
                    f' -N {job_name}'
                    f' python_shell.sh'
                    f' congenital_save.py'
                    f' {save_id} {out_dir} \'{description}\''
                    f' {gbdrid} {decomp_step} {save_info[0]} {save_info[1]}')
        subprocess.call(call, shell=True)

    # # ###########################################################################
    # # # heart failure due to congenital heart defects
    # # ###########################################################################

    if cause_name == "cong_heart":
        # hold = 'no_holds'
        hold = save_string
        hrtf_cn = "cong_heart_failure"
        hrtf_map = cmap[hrtf_cn]

        # add acause subdirectories to timestamped output folder
        hrtf_outdir = "{base}/{proc}".format(base=base, proc=hrtf_cn)
        # add me_id output directories to acause subdirectory and save
        mapdf = json_normalize(hrtf_map)
        trgs = mapdf.filter(regex=(".*trgs.*"))
        save_ids = trgs.values.tolist()[0]
        for me_id in save_ids:
            if not os.path.exists(os.path.join(hrtf_outdir, str(me_id))):
                os.makedirs(os.path.join(hrtf_outdir, str(me_id)))
        save_info = dict()
        for key, value in hrtf_map.items():
            if key == 'split': continue
            for subcause_name, subcause_dict in value.items():
                info_tuple = get_bundle_xwalk_version_for_best_model(
                    subcause_dict['srcs']['post_sqzd_me_id'],gbdrid, decomp_step)
                for sev, me_id in subcause_dict['trgs'].items():
                    save_info[me_id] = info_tuple

        hrtf_string = ''
        for location_id in most_detailed_locs:
            job_name = "heart_failure_{l}".format(l=location_id)
            hrtf_string = hrtf_string + ',' + job_name
            call = ('qsub  -hold_jid {hj} -l m_mem_free=2G -l fthread=2 -l archive -q long.q'
                    ' -cwd -P proj_nch'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' python_shell.sh'
                    ' hrt_fail_imp_cong.py'
                    '  \'{arg1}\' {arg2} {arg3} {arg4} {arg5} {arg6}'.format(
                        hj=hold, 
                        o=output_path, e=error_path, jn=job_name, 
                        arg1=json.dumps(hrtf_map), arg2=hrtf_outdir, 
                        arg3=location_id, arg4=cause_name,
                        arg5=gbdrid, arg6=decomp_step))
            subprocess.call(call, shell=True)

        """ Get version ids of models used severity calculations. Export to
        csv. This strategy assumes that no new dismod source models have been 
        marked best since the start of the core code. """
        srcs = mapdf.filter(regex=("^((?!other.*pre_sqzd_me_id).)*srcs.*"))
        src_ids = srcs.values.tolist()[0]
        df = get_best_model_versions(entity='modelable_entity',
            ids=src_ids, gbd_round_id=gbdrid, decomp_step = decomp_step)
        df.to_csv(os.path.join(hrtf_outdir,'model_version_ids.csv'), 
            index=False, encoding='utf-8')
        description = ("See csv in {}/{} for model_version_id"
            " information".format(os.path.basename(base), hrtf_cn))

        for save_id in save_ids:
            print(save_id)
            hold = "'heart_failure*'"
            job_name = "save_heart_failure_{svid}".format(cn=hrtf_cn, 
                svid=save_id)
            call = ('qsub  -hold_jid {hj} -l m_mem_free=120G -l fthread=60 -l archive -q all.q'
                        ' -cwd -P proj_nch'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' python_shell.sh'
                        ' congenital_save.py'
                        ' {arg1} {arg2} \'{arg3}\' {arg4} {arg5} {arg6} {arg7}'.format(
                            hj=hold, 
                            o=output_path, e=error_path, jn=job_name, 
                            arg1=save_id, arg2=hrtf_outdir, arg3=description,
                            arg4=gbdrid, arg5=decomp_step,
                            arg6=save_info[save_id][0], 
                            arg7=save_info[save_id][1]))

            subprocess.call(call, shell=True)
        save_string += hrtf_string
