'''
This part of the code sets up directories to store draws retrieved from 
the epi database and executes the calc_other.py, calc_denominator.py, 
congenital_core.py, hrt_fail_imp_cong.py, congenital_save.py, and diagnostic
plot scripts.
'''

import pandas as pd
import os
import shutil
import json
import re
import subprocess
from db_queries import get_best_model_versions
from db_queries import get_location_metadata
from datetime import datetime
import time


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

# Set up dictionaries that organize me_id data into the appropriate hierarchies 
cong_heart = {
    "name": {
        "type": "cong_heart"
    },
    "env": {
        "type": "envelope",
        "srcs": {
            "tot":"10437",
            "bundle": "628"
        }
    },
    "Critical malformations of great vessels, congenital valvular heart disease and patent ductus arteriosis": {
        "type": "sub_group",
        "srcs": {
            "tot": "10441",
            "bundle": "632"
        },
        "trgs": {
            "sqzd": "10936"
        }
    },
    "Ventricular septal defect and atrial septal defect": {
        "type": "sub_group",
        "srcs": {
            "tot": "10443",
            "bundle": "634"
        },
        "trgs": {
            "sqzd": "10937"
        }
    },
    "Single ventricle and single ventricle pathway heart defects": {
        "type": "sub_group",
        "srcs": {
            "tot": "10439",
            "bundle": "630"
        },
        "trgs": {
            "sqzd": "10935"
        }
    },
    "Severe congenital heart defects excluding single ventricle and single ventricle pathway": {
        "type": "sub_group",
        "srcs": {
            "tot": "10445",
            "bundle": "636"
        },
        "trgs": {
            "sqzd": "10938"
        }
    },
    "Other": {
        "type": "other",
        "srcs": {
            "tot": "10934",
            "bundle": "2978"
        },
        "trgs": {
            "sqzd": "10939"
        }
    }
}


cong_neural = {
    "name": {
        "type": "cong_neural"
    },
    "env": {
        "type": "envelope",
        "srcs": {
            "tot": "10415",
            "bundle": "608"
        }
    },
    "Spina Bifida": {
        "type": "sub_group",
        "srcs": {
            "tot": "10421",
            "bundle": "614"
        },
        "trgs": {
          "sqzd": "10940"
        }
    },
    "Anencephaly": {
        "type": "special",
        "srcs": {
            "dismod": "10417",
            "bundle": "610"
        },
        "trgs": {
          "custom": "15791"
        }
    },
    "Encephalocele": {
        "type": "sub_group",
        "srcs": {
            "tot": "10419",
            "bundle": "612"
        },
        "trgs": {
          "sqzd": "10941"
        }
    }
}

cong_msk = {
    "name": {
        "type": "cong_msk"
    },
    "env": {
        "type": "envelope",
        "srcs": {
            "tot": "10407",
            "bundle": "602"
        }
    },
    "Limb reduction deficits": {
        "type": "sub_group",
        "srcs": {
            "tot": "10409",
            "bundle": "604"
        },
        "trgs": {
          "sqzd": "10926"
        }
    },
    "Polydactyly and syndactyly": {
        "type": "sub_group",
        "srcs": {
            "tot": "10411",
            "bundle": "606"
        },
        "trgs": {
          "sqzd": "10927"
        }
    },
    "Other": {
        "type": "other",
        "srcs": {
            "tot": "10925",
            "bundle": "2972"
        },
        "trgs": {
          "sqzd": "10928"
        }
    }
}

cong_digestive = {
    "name": {
        "type": "cong_digestive"
    },
    "env": {
        "type": "envelope",
        "srcs": {
            "tot": "10427",
            "bundle": "620"
        }
    },
    "Congenital diaphragmatic hernia": {
        "type": "sub_group",
        "srcs": {
            "tot": "10429",
            "bundle": "622"
        },
        "trgs": {
          "sqzd": "10930"
        }
    },
    "Congenital atresia and/or stenosis of the digestive tract": {
        "type": "sub_group",
        "srcs": {
            "tot": "10433",
            "bundle": "626"
        },
        "trgs": {
          "sqzd": "10932"
        }
    },
    "Congenital malformations of the abdominal wall": {
        "type": "sub_group",
        "srcs": {
            "tot": "10431",
            "bundle": "624"
        },
        "trgs": {
          "sqzd": "10931"
        }
    },
    "Other": {
        "type": "other",
        "srcs": {
            "tot": "10929",
            "bundle": "2975"
        },
        "trgs": {
          "sqzd": "10933"
        }
    }
}

cong_chromo = {
    "name": {
        "type": "cong_chromo"
    },
    "env": {
        "type": "envelope",
        "srcs": {
            "tot": "18654",
            "bundle": "3029"
        }
    },
    "Down syndrome": {
        "type": "sub_group",
        "srcs": {
            "tot": "3246",
            "bundle": "436"
        },
        "trgs": {
          "sqzd": "18752"
        }
    },
    "Turner syndrome": {
        "type": "sub_group",
        "srcs": {
            "tot": "3247",
            "bundle": "437"
        },
        "trgs": {
          "sqzd": "18753"
        }
    },
    "Klinefelter syndrome": {
        "type": "sub_group",
        "srcs": {
            "tot": "3248",
            "bundle": "438"
        },
        "trgs": {
          "sqzd": "18754"
        }
    },
    "Edward Syndrome and Patau Syndrome": {
        "type": "sub_group",
        "srcs": {
            "tot": "10449",
            "bundle": "638"
        },
        "trgs": {
          "sqzd": "18756"
        }
    },
    "Other": {
        "type": "other",
        "srcs": {
            "tot": "3249",
            "bundle": "439"
        },
        "trgs": {
          "sqzd": "18755"
        }
    }
}

cong_heart_failure = {
    "name": {
        "type": "cong_heart_failure"
    },
    "heart failure": {
        "type": "split",
        "srcs": {
            "asymp": "20090",
            "mild": "2179",
            "moderate": "2180",
            "severe": "2181"
        }
    },
    "Critical malformations of great vessels, congenital valvular heart disease and patent ductus arteriosis": {
        "type": "sub_group",
        "srcs": {
            "tot": "10441",
            "sqzd": "10936"
        },
        "trgs": {
            "none": "15768",
            "asymp": "20021",
            "mild": "15769",
            "moderate": "15770",
            "severe": "15771"
        }
    },
    "Ventricular septal defect and atrial septal defect": {
        "type": "sub_group",
        "srcs": {
            "tot": "10443",
            "sqzd": "10937"
        },
        "trgs": {
            "none": "15773",
            "asymp": "20022",
            "mild": "15774",
            "moderate": "15775",
            "severe": "15776",
            "asymp_vsdasd": "11392"
        }
    },
    "Single ventricle and single ventricle pathway heart defects": {
        "type": "sub_group",
        "srcs": {
            "tot": "10439",
            "sqzd": "10935"
        },
        "trgs": {
            "none": "15760",
            "asymp": "20019",
            "mild": "15761",
            "moderate": "15762",
            "severe": "15763"
        }
    },
    "Severe congenital heart defects excluding single ventricle and single ventricle pathway": {
        "type": "sub_group",
        "srcs": {
            "tot": "10445",
            "sqzd": "10938"
        },
        "trgs": {
            "none": "15764",
            "asymp": "20020",
            "mild": "15765",
            "moderate": "15766",
            "severe": "15767"
        }
    },
    "Other": {
        "type": "other",
        "srcs": {
            "tot": "10934",
            "sqzd": "10939"
        },
        "trgs": {
            "none": "15756"
        }
    }
}

# make timestamped output folder
date_regex = re.compile('\W')
date_unformatted = str(datetime.now())[0:13]
c_date = date_regex.sub('_', date_unformatted)
base = "FILEPATH"

loc_meta = get_location_metadata(location_set_id=35, gbd_round_id=5)
loc_meta = loc_meta.loc[loc_meta.most_detailed==1, ['location_id', 'ihme_loc_id']]
most_detailed_locs = loc_meta.location_id.tolist()

for cause in [cong_heart, cong_digestive, cong_msk, cong_chromo, cong_neural]:
    me_map = cause
    cause_name = me_map['name']['type']

    ###########################################################################
    # subdirectories for core code
    ###########################################################################
    # add acause subdirectories to timestamped output folder
    out_dir = "{base}/{proc}".format(base=base, proc=cause_name)
    
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # add me_id output directories to acause subdirectory
    save_ids = []
    for _, v in me_map.items():
        outputs = v.get('trgs',{})
        for trg_key, me_id in outputs.items():
            if not os.path.exists(os.path.join(out_dir, str(me_id))):
                os.makedirs(os.path.join(out_dir, str(me_id)))
            save_ids.append(me_id)

    ###########################################################################
    # Calculate proportion other from claims data
    ###########################################################################
    if cause_name in ['cong_heart', 'cong_msk']:
        other_job_name = 'calc_other_{}'.format(cause_name)
        call = ('qsub -hold_jid {hj} -pe multi_slot 2'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' cluster_shell.sh'
                        ' calc_other.py'
                        ' \'{arg1}\''.format(hj='no_holds', 
                            o=output_path, e=error_path, 
                            jn=other_job_name, arg1=json.dumps(me_map)))
        subprocess.call(call, shell=True)
    
    ###########################################################################
    # core
    ###########################################################################
    # submit congenital_core job for each cause and form a string of job names
    job_string = ''
    for location_id in most_detailed_locs:
        if cause_name == 'cong_neural':
            # calcute the denominator needed for custom anencephaly prevalence
            # calculation
            ihme_loc_id = loc_meta.loc[loc_meta.location_id==location_id,'ihme_loc_id'].item()
            denom_job_name = 'denom_calc_{l}_{cn}'.format(l=location_id, cn=cause_name)
            share_dir = "FILEPATH"
            call = ('qsub -hold_jid {hj} -pe multi_slot 2'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' cluster_shell.sh'
                    ' calc_denominator.py'
                    ' {arg1} {arg2} {arg3}'.format(hj='no_holds', 
                        o=output_path, e=error_path, jn=denom_job_name, 
                        arg1=share_dir, arg2=location_id, arg3=ihme_loc_id))
        
            subprocess.call(call, shell=True)
            hold = denom_job_name
        elif cause_name in ['cong_heart', 'cong_msk']:
            hold = other_job_name
        else:
            hold = 'no_holds'

        job_name = 'squeeze_{l}_{cn}'.format(l=location_id, cn=cause_name)
        job_string = job_string + ',' + job_name
        call = ('qsub -hold_jid {hj} -pe multi_slot 2'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' cluster_shell.sh'
                    ' congenital_core.py'
                    ' \'{arg1}\' {arg2} {arg3} {arg4}'.format(hj=hold, 
                        o=output_path, e=error_path, jn=job_name, 
                        arg1=json.dumps(me_map), arg2=out_dir, 
                        arg3=location_id, arg4=cause_name))
        subprocess.call(call, shell=True)

    ###########################################################################
    # save
    ###########################################################################
    # get version ids of models used in core calculations
    # upload this information in the save description
    description = "SAVE DESCRIPTION"

    model_version_list = []
    anceph = []
    for mapper_key, v in me_map.items():
        outputs = v.get("srcs",{})
        for src_key, me_id in outputs.items():
            if src_key == "tot" and mapper_key != "Other":
                model_version_list.append(me_id)
            elif src_key == "dismod":
                anceph.append(me_id)

    df = get_best_model_versions(entity='modelable_entity',
        ids=model_version_list, gbd_round_id=5)
    model_ids_str_for_save = ''
    for index,row in df.iterrows():
        model_ids_str_for_save += ' meid {}, mvid {};'.format(row.modelable_entity_id,row.model_version_id)
    description += model_ids_str_for_save[:-1] + "."
    if anceph:
        df = get_best_model_versions(entity='modelable_entity',ids=anceph, 
            gbd_round_id=5)
        description += " Anenceph used meid {}, mvid {}.".format(df.modelable_entity_id.item(),df.model_version_id.item())

    # save custom results
    save_string = ''
    for save_id in save_ids:
        job_name = "{cn}_save_{svid}".format(cn=cause_name, svid=save_id)
        save_string = save_string + ',' + job_name
        call = ('qsub  -hold_jid {hj} -pe multi_slot 15'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' cluster_shell.sh'
                    ' congenital_save.py'
                    ' {arg1} {arg2} \'{arg3}\''.format(hj=job_string, 
                        o=output_path, e=error_path, jn=job_name, 
                        arg1=save_id, arg2=out_dir, arg3=description))

        subprocess.call(call, shell=True)

    ###########################################################################
    # heart failure due to congenital heart defects
    ###########################################################################

    if cause_name == "cong_heart":
        hold = save_string
        hrtf_map = cong_heart_failure
        hrtf_cn = hrtf_map['name']['type']

        # add acause subdirectories to timestamped output folder
        hrtf_outdir = "{base}/{proc}".format(base=base, proc=hrtf_cn)
        # add me_id output directories to acause subdirectory and save
        save_ids = []
        for _, v in hrtf_map.items():
            outputs = v.get('trgs',{})
            for trg_key, me_id in outputs.items():
                if not os.path.exists(os.path.join(hrtf_outdir, str(me_id))):
                    os.makedirs(os.path.join(hrtf_outdir, str(me_id)))
                save_ids.append(me_id)
        
        hrtf_string = ''
        for location_id in most_detailed_locs:
            job_name = "heart_failure_{l}".format(l=location_id)
            hrtf_string = hrtf_string + ',' + job_name
            call = ('qsub  -hold_jid {hj} -pe multi_slot 5'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' cluster_shell.sh'
                    ' hrt_fail_imp_cong.py'
                    '  \'{arg1}\' {arg2} {arg3} {arg4}'.format(hj=hold, 
                        o=output_path, e=error_path, jn=job_name, 
                        arg1=json.dumps(hrtf_map), arg2=hrtf_outdir, 
                        arg3=location_id, arg4=cause_name))

            subprocess.call(call, shell=True)

        for save_id in save_ids:
            hold = hrtf_string
            job_name = "save_heart_failure_{svid}".format(cn=hrtf_cn, 
                svid=save_id)
            call = ('qsub  -hold_jid {hj} -pe multi_slot 15'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' cluster_shell.sh'
                        ' congenital_save.py'
                        ' {arg1} {arg2} \'{arg3}\''.format(hj=hold, 
                            o=output_path, e=error_path, jn=job_name, 
                            arg1=save_id, arg2=hrtf_outdir, arg3=''))

            subprocess.call(call, shell=True)
    time.sleep(30)



